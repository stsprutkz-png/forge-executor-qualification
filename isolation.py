"""Production path accepts a qualified ROOTLESS Podman backend only."""
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import uuid
import math
import threading
from processes import command, Deadline, Fault


class PodmanBackend:
    assurance = 'ROOTLESS_PODMAN'

    def __init__(self, image, scope):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', image): raise Fault('IMMUTABLE_IMAGE_REQUIRED')
        self.image = image
        self.binary = shutil.which('podman')
        self.validated = False
        self.evidence = None
        if not re.fullmatch(r'[0-9a-f]{64}',scope):raise Fault('EXECUTOR_SCOPE_REQUIRED')
        self.scope=scope
        self.local=threading.local()

    def begin(self, operation_id, deadline):
        self.validated=False
        self.local.operation_id=operation_id
        self.cleanup_operation(operation_id,deadline)

    def cleanup_operation(self, operation_id, deadline):
        if not self.binary or os.geteuid()==0:raise Fault('ROOTLESS_HOST_UNAVAILABLE')
        r=command([self.binary,'ps','--all','--filter','label=forge.scope='+self.scope,
                   '--filter','label=forge.operation='+operation_id,'--format={{.Names}}'],deadline)
        if r['exit_code']:raise Fault('CONTAINER_INVENTORY_UNAVAILABLE')
        for name in r['stdout'].splitlines():
            if not re.fullmatch(r'forge-q-[0-9a-f]{32}',name):raise Fault('CONTAINER_LABEL_INVALID')
            gone=command([self.binary,'rm','--force','--time=0',name],deadline)
            if gone['exit_code']:raise Fault('CONTAINER_CLEANUP_UNPROVEN')

    def argv(self, workspace, name, args, timeout=10):
        return [self.binary, 'run', '--rm', '--pull=never', '--name', name,
                '--label=forge.scope='+self.scope,
                '--label=forge.operation='+getattr(self.local,'operation_id','preflight'),
                '--timeout='+str(max(1,math.ceil(timeout))),
                '--network=none', '--no-hosts', '--read-only',
                '--read-only-tmpfs=false', '--user=65532:65532', '--cap-drop=all',
                '--security-opt=no-new-privileges', '--memory=256m', '--memory-swap=256m',
                '--cpus=1', '--pids-limit=32', '--cgroupns=private', '--shm-size=1m',
                '--tmpfs=/scratch:rw,nosuid,nodev,noexec,size=16m,mode=1777',
                '--volume', str(Path(workspace).resolve())+':/work:ro',
                '--workdir=/scratch', '--env=PYTHONDONTWRITEBYTECODE=1',
                self.image, 'python', '-I', *args]

    def _run(self, workspace, args, deadline):
        name = 'forge-q-' + uuid.uuid4().hex
        try:
            return command(self.argv(workspace,name,args,deadline.remaining()),deadline)
        finally:
            # Removes the container/cgroup, including descendants after CLI timeout.
            # Cleanup has its own bounded reserve, no silently persistent workload.
            try:
                removed=command([self.binary,'rm','--force','--time=0',name],Deadline(3))
                if removed['exit_code'] != 0:
                    exists=command([self.binary,'container','exists',name],Deadline(2))
                    if exists['exit_code'] != 1: raise Fault('CONTAINER_CLEANUP_UNPROVEN')
            except OSError as exc: raise Fault('CONTAINER_CLEANUP_UNPROVEN') from exc

    def preflight(self, deadline):
        if not self.binary or os.geteuid()==0: raise Fault('ROOTLESS_HOST_UNAVAILABLE')
        r=command([self.binary,'info','--format=json'],deadline)
        try: info=json.loads(r['stdout']); host=info['host']; security=host['security']
        except (ValueError,KeyError,TypeError):raise Fault('PODMAN_INFO_INVALID')
        if r['exit_code'] or security.get('rootless') is not True or host.get('cgroupVersion') != 'v2':
            raise Fault('ROOTLESS_CGROUP_V2_REQUIRED')
        if not security.get('seccompEnabled'): raise Fault('SECCOMP_REQUIRED')
        with tempfile.TemporaryDirectory(prefix='forge-isolation-probe-') as tmp:
            workspace=Path(tmp)/'visible';workspace.mkdir();workspace.chmod(0o755)
            Path(tmp).chmod(0o755)
            outside=Path(tmp)/'outside-canary';outside.write_text('synthetic marker')
            source = '''import os,json,socket
from pathlib import Path
def write_blocked(path):
 try: Path(path).write_text('probe');return False
 except OSError:return True
def cg(name):return Path('/sys/fs/cgroup/'+name).read_text().strip()
status=dict(line.split(':',1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
q,p=map(int,cg('cpu.max').split())
r={'uid':os.geteuid(),'caps_zero':int(status['CapEff'].strip(),16)==0,
   'no_new_privs':status['NoNewPrivs'].strip()=='1','seccomp':status['Seccomp'].strip()=='2',
   'readonly_runtime':write_blocked('/usr/local/forbidden-probe'),
   'readonly_work':write_blocked('/work/forbidden-probe'),
   'outside_hidden':not Path(OUTSIDE).exists(),
   'memory_bounded':0<int(cg('memory.max'))<=268435456,
   'pids_bounded':0<int(cg('pids.max'))<=32,'cpu_bounded':0<q<=p,
   'only_loopback':set(Path('/sys/class/net').iterdir())=={Path('/sys/class/net/lo')}}
Path('/scratch/allowed').write_text('ok');r['scratch_writable']=True
s=socket.socket();s.settimeout(0.2)
try:r['egress_blocked']=s.connect_ex(('198.51.100.1',443))!=0
finally:s.close()
print(json.dumps(r,sort_keys=True))
'''.replace('OUTSIDE',repr(str(outside)))
            (workspace/'preflight.py').write_text(source)
            r=self._run(workspace,['/work/preflight.py'],deadline)
            try: evidence=json.loads(r['stdout'])
            except ValueError: raise Fault('ISOLATION_PROBE_INVALID')
            expected={'uid','caps_zero','no_new_privs','seccomp','readonly_runtime','readonly_work','outside_hidden',
                      'memory_bounded','pids_bounded','cpu_bounded','only_loopback','scratch_writable','egress_blocked'}
            if r['exit_code'] or set(evidence)!=expected or evidence.get('uid') != 65532 or not all(v is True for k,v in evidence.items() if k!='uid'):
                raise Fault('ISOLATION_PROBE_FAILED')
            self.evidence={'image_id':self.image,'rootless':True,'cgroup_version':'v2','behavior':evidence}
            self.validated=True
        return self.evidence

    def run(self, workspace, script, input_name, deadline):
        if not self.validated: raise Fault('ISOLATION_NOT_QUALIFIED')
        return self._run(workspace,['/work/'+script,'/work/'+input_name],deadline)
