"""REAL deployment driver: JSON -> worker CLI -> delivered bytes. No fixture backend."""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from executor import sha, canonical
from sample_tasks import task


def run(image,output):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    state=output/'state';worker=Path(__file__).parent/'worker.py'
    requests=[]
    def send(request):
        start=time.monotonic()
        r=subprocess.run([sys.executable,'-I',str(worker),'--root',str(state),'--image',image],
                         input=canonical(request),capture_output=True,timeout=130)
        if r.returncode:raise RuntimeError('WORKER_INTERFACE_FAILED')
        result=json.loads(r.stdout)
        record={'action':request['action'],'request_sha256':sha(canonical(request)),
                'elapsed_seconds':time.monotonic()-start,'status':result.get('status')}
        requests.append(record)
        return result
    start=time.monotonic()
    c=send({'action':'SUBMIT','task':task(seconds=120)})
    if c.get('status')!='VERIFIED_ARTIFACT':raise RuntimeError('CREATE_NOT_ISOLATED_VERIFIED:'+json.dumps(c))
    u=send({'action':'SUBMIT','task':task(task_id='update-1',mode='UPDATE',base=c['sha256'],seconds=120)})
    if u.get('status')!='VERIFIED_ARTIFACT':raise RuntimeError('UPDATE_NOT_ISOLATED_VERIFIED:'+json.dumps(u))
    f={'action':'FETCH','project_id':'csv-pilot','operation_id':u['operation_id']}
    a=send(f);b=send(f)
    assert a['artifact_base64']==b['artifact_base64'] and a['sha256']==u['sha256']
    data=base64.b64decode(a['artifact_base64']);assert sha(data)==u['sha256']
    (output/'product.zip').write_bytes(data)
    repeated=send({'action':'SUBMIT','task':task(task_id='update-1',mode='UPDATE',base=c['sha256'],seconds=120)})
    assert repeated['reused'] and repeated['sha256']==u['sha256']
    bad=task(project='negative-marker',seconds=120);bad['files']={'summary.py':"print('FORGE_REQUIREMENT:R1:PASS')\n"}
    negative=send({'action':'SUBMIT','task':bad});assert negative['status']=='FAILED'
    broken=task(project='executor-refusal',seconds=120);broken['files']={'summary.py':"raise RuntimeError('refusal')\n"}
    refusal=send({'action':'SUBMIT','task':broken});assert refusal['status']=='FAILED'
    slow=task(project='timeout-tree',seconds=5)
    slow['files']={'summary.py':'import subprocess,sys,time\nsubprocess.Popen([sys.executable,"-c","import os,time;os.setsid();time.sleep(60)"])\ntime.sleep(60)\n'}
    timeout=send({'action':'SUBMIT','task':slow})
    assert timeout['status'] in ('FAILED','OUTCOME_UNKNOWN')
    from isolation import PodmanBackend
    from processes import Deadline
    scope=sha(str(state.resolve()).encode())
    timeout_op=sha(canonical([slow['project_id'],slow['task_id'],sha(canonical(slow))]))
    PodmanBackend(image,scope).cleanup_operation(timeout_op,Deadline(5))
    # Real hard exit after durable verification, before SQLite project promotion.
    crash=task(project='crash-pilot',seconds=120)
    child_code='''import os,sys,json
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from executor import Executor,sha
from isolation import PodmanBackend
def hook(stage):
 if stage=='verified_before_commit':os._exit(74)
root=Path(sys.argv[2]).resolve()
backend=PodmanBackend(sys.argv[3],sha(str(root).encode()))
Executor(root,backend).submit(json.loads(sys.argv[4]),fault_hook=hook)
'''
    stopped=subprocess.run([sys.executable,'-I','-c',child_code,str(Path(__file__).parent),str(state),image,
                            canonical(crash).decode()],capture_output=True,timeout=130)
    assert stopped.returncode==74
    crash_op=sha(canonical([crash['project_id'],crash['task_id'],sha(canonical(crash))]))
    observed=send({'action':'STATUS','project_id':'crash-pilot','operation_id':crash_op})
    assert observed['phase']=='VERIFIED'
    recovered=send({'action':'SUBMIT','task':crash})
    assert recovered['reused'] and recovered['status']=='VERIFIED_ARTIFACT'
    assert not list((state/'staging').iterdir())
    # Independent provider evidence is required outside this script. Flags alone are not proof.
    result={'status':'ISOLATED_HOST_CYCLE','remote_proven':False,'create':{k:v for k,v in c.items() if k!='artifact_base64'},
            'update':{k:v for k,v in u.items() if k!='artifact_base64'},'negative':negative,'refusal':refusal,'timeout':timeout,
            'request_records':requests,'product_sha256':sha(data),'elapsed_seconds':time.monotonic()-start,
            'interruption':{'exit_code':stopped.returncode,'phase_before_resume':observed['phase'],
                            'reused_after_resume':recovered['reused'],'orphan_staging_after':0},
            'model_api_calls':0,'model_api_incremental_cost_usd':0,'infrastructure_invoice_observed':False,
            'provider_context':{k:os.environ.get(k) for k in ['GITHUB_RUN_ID','GITHUB_JOB','GITHUB_SHA','GITHUB_REPOSITORY']}}
    (output/'remote-cycle.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--image',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    r=run(a.image,a.output);print(json.dumps({'status':r['status'],'sha256':r['product_sha256'],'elapsed_seconds':r['elapsed_seconds']}))
