"""Actual API-only cycle. Missing credentials => BLOCKED, never substitutes sample code."""
import argparse
import base64
import json
import os
from pathlib import Path
import time
from executor import Executor, sha
from isolation import PodmanBackend
from contracts import verify
from processes import Deadline, Fault
from model_adapter import Adapter, ENDPOINTS

def cycle(root,image,models):
    output=Path(root).resolve();output.mkdir(parents=True,exist_ok=True)
    if set(models)!={'deepseek','openrouter'}:raise Fault('MODELS_CONFIG_INVALID')
    missing=[n for n,(_,env) in ENDPOINTS.items() if not os.environ.get(env)]
    if missing:return {'status':'BLOCKED','reason':'PROVIDER_CREDENTIAL_MISSING','providers':missing,'model_calls':0,'llm_cycle_pass':False}
    adapter=Adapter(output/'controller-private');state=output/'executor-state'
    backend=PodmanBackend(image,sha(str(state).encode()));executor=Executor(state,backend)
    started=time.monotonic();receipts=[];c=None
    for mode,order in [('CREATE',['deepseek','openrouter']),('UPDATE',['openrouter','deepseek'])]:
        spec={'project_id':'llm-csv-pilot','task_id':mode.lower()+'-1','mode':mode,
              'oracle_version':'csv-v1' if mode=='CREATE' else 'csv-v2','deadline_seconds':120}
        if c:
            files=executor._archive(state/'products'/(c['operation_id']+'.zip'),c['sha256'])
            spec.update(base_sha256=c['sha256'],baseline_files={k:v.decode() for k,v in files.items()})
        prepared=adapter.prepare(spec,[(n,models[n]) for n in order])
        if 'task' not in prepared:return {**prepared,'llm_cycle_pass':False}
        if prepared['receipt'].get('provenance')!='LIVE_PROVIDER_API':raise Fault('REAL_API_PROVENANCE_REQUIRED')
        r=executor.submit(prepared['task'])
        if r.get('status')!='VERIFIED_ARTIFACT':return {'status':'FAILED','reason':r.get('reason'),'llm_cycle_pass':False}
        c=r;receipts.append({'task_sha256':prepared['task_sha256'],'raw_model_sha256':prepared['raw_model_sha256'],
                            'prompt_sha256':prepared['prompt_sha256'],'provider_receipt':prepared['receipt'],
                            'attempts':prepared['attempts'],'executor':{k:v for k,v in r.items() if k!='artifact_base64'}})
    a=executor.fetch('llm-csv-pilot',c['operation_id']);b=executor.fetch('llm-csv-pilot',c['operation_id'])
    data=base64.b64decode(a['artifact_base64']);assert data==base64.b64decode(b['artifact_base64']) and sha(data)==c['sha256']
    repeated=executor.submit(prepared['task']);assert repeated['reused'] and repeated['sha256']==c['sha256']
    product=output/'product.zip';product.write_bytes(data)
    # A new backend/state context checks delivered bytes using controller-owned oracle.
    check=output/'independent-check';check.mkdir(exist_ok=True)
    checker=Executor(check/'state',PodmanBackend(image,sha(str(check).encode())))
    workspace=check/'product';checker._write_files(workspace,checker._archive(product,c['sha256']))
    deadline=Deadline(120);checker.backend.begin('0'*64,deadline);isolation=checker.backend.preflight(deadline)
    checks=verify(workspace,'csv-v2',checker.backend,deadline);assert checks['ok']
    result={'status':'LIVE_MODEL_ISOLATED_CYCLE','remote_provider_verified_externally':False,'llm_cycle_pass':False,
            'receipts':receipts,'independent_checks':checks,'isolation':isolation,'downloaded_twice_equal':True,
            'update_reused':True,'product_sha256':sha(data),'elapsed_seconds':time.monotonic()-started,
            'note':'Host cycle evidence only; remote attestation and independent host delivery required externally.'}
    for name in ['cycle-evidence.json','remote-cycle.json']:
        (output/name).write_text(json.dumps(result,indent=2))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--image',required=True);p.add_argument('--models',required=True)
    a=p.parse_args()
    try:r=cycle(a.root,a.image,json.loads(Path(a.models).read_text()))
    except (Fault,ValueError,OSError) as e:r={'status':'FAILED','reason':type(e).__name__,'llm_cycle_pass':False}
    print(json.dumps({k:v for k,v in r.items() if k!='receipts'}));raise SystemExit(0 if r['status']=='LIVE_MODEL_ISOLATED_CYCLE' else 2)
