import base64
from concurrent.futures import ThreadPoolExecutor
import ctypes
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile
from unittest.mock import patch
from executor import Executor, safe
from isolation import PodmanBackend
from processes import Fault, command, Deadline
from sample_tasks import task
from test_fixtures import FixtureBackend


class ExecutorTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'state'
        self.backend=FixtureBackend();self.executor=Executor(self.root,self.backend)
    def tearDown(self):self.tmp.cleanup()

    def test_create_update_fetch_preserves_identity_and_bytes(self):
        created=self.executor.submit(task())
        self.assertEqual(created['status'],'VERIFIED_TEST_FIXTURE')
        updated=self.executor.submit(task(task_id='update-1',mode='UPDATE',base=created['sha256']))
        self.assertEqual(updated['project_id'],'csv-pilot')
        self.assertEqual(updated['status'],'VERIFIED_TEST_FIXTURE')
        calls=self.backend.calls
        a=self.executor.fetch('csv-pilot',updated['operation_id']);b=self.executor.fetch('csv-pilot',updated['operation_id'])
        self.assertEqual(a['artifact_base64'],b['artifact_base64']);self.assertEqual(self.backend.calls,calls)
        with zipfile.ZipFile(io.BytesIO(base64.b64decode(a['artifact_base64']))) as z:self.assertIn('README.md',z.namelist())
        self.assertEqual(hashlib.sha256(base64.b64decode(a['artifact_base64'])).hexdigest(),updated['sha256'])
        self.assertEqual(len(updated['delivery_checks']['cases']),7)

    def test_repeat_completed_does_not_execute(self):
        t=task();first=self.executor.submit(t);calls=self.backend.calls
        second=Executor(self.root,self.backend).submit(t)
        self.assertTrue(second['reused']);self.assertEqual(first['sha256'],second['sha256'])
        self.assertEqual(self.backend.calls,calls)

    def test_fetch_remains_available_without_runtime_recovery(self):
        from worker import child
        r=self.executor.submit(task())
        with patch.object(PodmanBackend,'cleanup_operation',side_effect=AssertionError('read must not recover writes')):
            fetched=child(self.root,'sha256:'+'0'*64,{'action':'FETCH','project_id':'csv-pilot','operation_id':r['operation_id']})
        self.assertEqual(fetched['sha256'],r['sha256'])

    def test_identity_and_baseline_cannot_be_redirected(self):
        first=self.executor.submit(task())
        changed=task();changed['files']['README.md']='different'
        with self.assertRaisesRegex(Fault,'TASK_IDENTITY_COLLISION'):self.executor.submit(changed)
        redirected=self.executor.submit(task(project='other',task_id='update-x',mode='UPDATE',base=first['sha256']))
        self.assertEqual(redirected['reason'],'BASELINE_CHANGED')
        stale=self.executor.submit(task(task_id='update-stale',mode='UPDATE',base='0'*64))
        self.assertEqual(stale['reason'],'BASELINE_CHANGED')
        self.assertEqual(self.executor.head('csv-pilot')['sha256'],first['sha256'])

    def test_concurrent_same_task_only_runs_once(self):
        t=task()
        with ThreadPoolExecutor(max_workers=2) as pool:
            a=pool.submit(self.executor.submit,t);b=pool.submit(self.executor.submit,t);results=[a.result(),b.result()]
        self.assertEqual(self.backend.calls,14)
        self.assertEqual(sum(r['reused'] for r in results),1)
        self.assertEqual(results[0]['operation_id'],results[1]['operation_id'])

    def test_false_marker_and_worker_error_are_not_success(self):
        for i,source in enumerate(["print('FORGE_REQUIREMENT:R1:PASS')\n","raise RuntimeError('executor failed')\n"]):
            t=task(project='negative-'+str(i));t['files']={'summary.py':source}
            r=self.executor.submit(t);self.assertEqual(r['status'],'FAILED');self.assertEqual(r['reason'],'BEHAVIOR_FAILED')
        self.assertIsNone(self.executor.head('negative-0'))

    def test_global_deadline_bounds_whole_oracle(self):
        t=task(seconds=0.15);t['files']={'summary.py':'import time; time.sleep(30)\n'}
        start=time.monotonic();r=self.executor.submit(t)
        self.assertEqual(r['reason'],'DEADLINE_EXCEEDED');self.assertLess(time.monotonic()-start,1)
        self.assertEqual(self.backend.calls,1)
        self.assertFalse(list((self.root/'staging').iterdir()))

    def test_verified_crash_recovers_without_reexecution(self):
        # BaseException simulates a lost supervisor after durable VERIFIED result.
        class Interrupted(BaseException):pass
        def hook(stage):
            if stage=='verified_before_commit':raise Interrupted()
        with self.assertRaises(Interrupted):self.executor.submit(task(),fault_hook=hook)
        calls=self.backend.calls
        r=Executor(self.root,self.backend).submit(task())
        self.assertEqual(r['status'],'VERIFIED_TEST_FIXTURE');self.assertTrue(r['reused'])
        self.assertEqual(self.backend.calls,calls)

    def test_hard_exit_leaves_owned_staging_then_cleanup_removes(self):
        code='''import os,sys
sys.path.insert(0,sys.argv[1])
from executor import Executor
from sample_tasks import task
from test_fixtures import FixtureBackend
def hook(stage):
 if stage=='staged':os._exit(73)
Executor(sys.argv[2],FixtureBackend()).submit(task(seconds=30),fault_hook=hook)
'''
        p=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).parent),str(self.root)],timeout=5)
        self.assertEqual(p.returncode,73);self.assertTrue(list((self.root/'staging').iterdir()))
        removed=self.executor.cleanup();self.assertEqual(len(removed),1)
        self.assertFalse(list((self.root/'staging').iterdir()))
        r=self.executor.submit(task(seconds=30));self.assertEqual(r['status'],'VERIFIED_TEST_FIXTURE')

    def test_corrupted_artifact_never_replayed_as_success(self):
        r=self.executor.submit(task());(self.root/'products'/(r['operation_id']+'.zip')).write_bytes(b'changed')
        with self.assertRaisesRegex(Fault,'ARTIFACT_CHANGED'):self.executor.fetch('csv-pilot',r['operation_id'])

    def test_contract_downgrade_cannot_discard_update(self):
        c=self.executor.submit(task());u=self.executor.submit(task(task_id='u',mode='UPDATE',base=c['sha256']))
        down=task(task_id='down',mode='UPDATE',base=u['sha256']);down['oracle_version']='csv-v1'
        r=self.executor.submit(down);self.assertEqual(r['reason'],'CONTRACT_DOWNGRADE')

    def test_deadline_survives_restart_and_does_not_reset(self):
        class Interrupted(BaseException):pass
        def hook(stage):
            if stage=='staged':raise Interrupted()
        t=task(seconds=0.1)
        with self.assertRaises(Interrupted):self.executor.submit(t,fault_hook=hook)
        time.sleep(0.12)
        r=Executor(self.root,self.backend).submit(t)
        self.assertEqual(r['reason'],'DEADLINE_EXCEEDED');self.assertEqual(self.backend.calls,0)

    def test_hard_exit_after_verified_recovers_without_program_calls(self):
        code='''import os,sys
sys.path.insert(0,sys.argv[1])
from executor import Executor
from sample_tasks import task
from test_fixtures import FixtureBackend
def hook(stage):
 if stage=='verified_before_commit':os._exit(74)
Executor(sys.argv[2],FixtureBackend()).submit(task(seconds=30),fault_hook=hook)
'''
        p=subprocess.run([sys.executable,'-I','-c',code,str(Path(__file__).parent),str(self.root)],timeout=5)
        self.assertEqual(p.returncode,74);self.assertIsNone(self.executor.head('csv-pilot'))
        self.executor.cleanup()
        r=self.executor.submit(task(seconds=30))
        self.assertTrue(r['reused']);self.assertEqual(self.backend.calls,0)
        self.assertFalse(list((self.root/'staging').iterdir()))

    def test_output_limit_is_failure(self):
        t=task();t['files']={'summary.py':"print('x'*200000)\n"}
        r=self.executor.submit(t);self.assertEqual(r['reason'],'OUTPUT_LIMIT')

    def test_new_task_after_failed_update_preserves_head(self):
        c=self.executor.submit(task())
        bad=task(task_id='bad-u',mode='UPDATE',base=c['sha256']);bad['files']={'summary.py':"print('PASS')\n"}
        r=self.executor.submit(bad);self.assertEqual(r['status'],'FAILED')
        self.assertEqual(self.executor.head('csv-pilot')['sha256'],c['sha256'])
        good=self.executor.submit(task(task_id='good-u',mode='UPDATE',base=c['sha256']))
        self.assertEqual(good['status'],'VERIFIED_TEST_FIXTURE')

    def test_path_boundary(self):
        for name in ['../x','/opt/Forge/x','x\\y','C:/x','.env','a/../x','a//x']:
            with self.subTest(name=name),self.assertRaises(Fault):safe(name)

    def test_production_isolation_unavailable_fails_before_candidate(self):
        backend=PodmanBackend('sha256:'+'0'*64,'0'*64)
        with patch('isolation.os.geteuid',return_value=0):
            r=Executor(self.root,backend).submit(task())
        self.assertEqual(r['status'],'FAILED');self.assertEqual(r['reason'],'ROOTLESS_HOST_UNAVAILABLE')
        self.assertIsNone(self.executor.head('csv-pilot'))

    def test_isolation_configuration_has_no_dns_conflict_or_host_mount(self):
        backend=PodmanBackend('sha256:'+'0'*64,'0'*64);backend.binary='podman'
        argv=backend.argv('/owned-work','forge-q-'+'a'*32,['/work/summary.py'],1)
        for flag in ['--network=none','--read-only','--user=65532:65532','--memory=256m','--cpus=1','--pids-limit=32','--cap-drop=all','--pull=never']:
            self.assertIn(flag,argv)
        self.assertFalse(any(x.startswith('--dns') for x in argv))
        self.assertEqual(argv.count('--volume'),1)

    def test_process_timeout_kills_and_reaps_own_descendants(self):
        # Adopt our own children; current /proc PID namespace differs, so use waitpid.
        libc=ctypes.CDLL(None,use_errno=True)
        self.assertEqual(libc.prctl(36,1,0,0,0),0)
        pidfile=Path(self.tmp.name)/'pid'
        source=('import subprocess,sys,time\nfrom pathlib import Path\n'
                'p=subprocess.Popen([sys.executable,"-I","-c","import time;time.sleep(30)"])\n'
                f'Path({str(pidfile)!r}).write_text(str(p.pid))\n'
                'time.sleep(30)\n')
        with self.assertRaisesRegex(Fault,'DEADLINE_EXCEEDED'):
            command([sys.executable,'-I','-c',source],Deadline(0.25))
        pid=int(pidfile.read_text());until=time.monotonic()+1
        while True:
            ended,_=os.waitpid(pid,os.WNOHANG)
            if ended==pid:break
            self.assertLess(time.monotonic(),until);time.sleep(0.01)

if __name__=='__main__':unittest.main(verbosity=2)
