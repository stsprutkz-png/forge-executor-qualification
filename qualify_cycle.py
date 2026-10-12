"""Qualification driver. --fixture is local control testing; never remote PASS."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
from executor import Executor, sha
from isolation import PodmanBackend
from processes import Deadline, Fault
from sample_tasks import task


def qualify(root,backend):
    began=time.monotonic();executor=Executor(root/'state',backend);executor.cleanup()
    create_task=task(seconds=120);created=executor.submit(create_task)
    if created['status'] not in ('VERIFIED_ARTIFACT','VERIFIED_TEST_FIXTURE'):
        return {'status':'NOT_QUALIFIED','failure':created,'remote_proven':False}
    update_task=task(task_id='update-1',mode='UPDATE',base=created['sha256'],seconds=120)
    updated=executor.submit(update_task)
    if updated['status'] not in ('VERIFIED_ARTIFACT','VERIFIED_TEST_FIXTURE'):
        return {'status':'NOT_QUALIFIED','failure':updated,'remote_proven':False}
    one=executor.fetch('csv-pilot',updated['operation_id']);two=executor.fetch('csv-pilot',updated['operation_id'])
    assert one['artifact_base64']==two['artifact_base64']
    products=root/'products';products.mkdir(exist_ok=True)
    for label,result in [('create',created),('update',updated)]:
        data=base64.b64decode(result.pop('artifact_base64'));assert sha(data)==result['sha256']
        (products/(label+'.zip')).write_bytes(data)
    bad=task(project='false-pass',seconds=120);bad['files']={'summary.py':"print('FORGE_REQUIREMENT:R1:PASS')\n"}
    rejected=executor.submit(bad);assert rejected['status']=='FAILED'
    stopped=task(project='timeout-pilot',seconds=1);stopped['files']={'summary.py':'import time;time.sleep(30)\n'}
    timeout=executor.submit(stopped);assert timeout['status']=='FAILED'
    result={'status':'CONTROL_TESTS_ONLY' if backend.assurance!='ROOTLESS_PODMAN' else 'ISOLATED_HOST_CYCLE',
            'remote_proven':False,'create':created,'update':updated,'false_pass':rejected,'timeout':timeout,
            'retrieval_hash_equal':sha(base64.b64decode(one['artifact_base64']))==sha(base64.b64decode(two['artifact_base64'])),
            'elapsed_seconds':time.monotonic()-began,'model_api_calls':0,'model_api_incremental_cost_usd':0,
            'infrastructure_invoice_observed':False,'runtime':platform.python_version(),
            'note':'Host cycle requires independent provider run/job/source and cross-host artifact retrieval before remote qualification.'}
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True)
    parser.add_argument('--fixture',action='store_true');parser.add_argument('--image');args=parser.parse_args()
    root=Path(args.root).resolve();root.mkdir(parents=True,exist_ok=True)
    if args.fixture:
        from test_fixtures import FixtureBackend
        backend=FixtureBackend()
    else:
        backend=PodmanBackend(args.image,sha(str(root/'state').encode()))
    result=qualify(root,backend)
    (root/'cycle-results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in result.items() if k not in ('create','update')},ensure_ascii=False,indent=2))
    sys.exit(0 if result['status'] in ('CONTROL_TESTS_ONLY','ISOLATED_HOST_CYCLE') else 1)
