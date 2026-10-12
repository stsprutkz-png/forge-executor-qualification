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


# Exact synthetic JSON code blocks copied from authenticated DeepSeek web UI.
# Replaying these bytes is not an API call; external provenance must be checked.
WEB_CAPTURES = {'CREATE': '{"files":{"summary.py":"import csv\\nimport sys\\nimport json\\nfrom decimal import Decimal, InvalidOperation, getcontext\\n\\n\\ndef main():\\n    if len(sys.argv) != 2:\\n        print(\\"Usage: python summary.py input.csv\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n    path = sys.argv[1]\\n\\n    try:\\n        with open(path, \\"r\\", encoding=\\"utf-8-sig\\", newline=\\"\\") as f:\\n            reader = csv.DictReader(f)\\n            if reader.fieldnames is None:\\n                print(\\"Error: missing header\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            fieldnames = [name.strip() if name is not None else name for name in reader.fieldnames]\\n            if \\"amount\\" not in fieldnames:\\n                print(\\"Error: missing amount column\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            amount_index = fieldnames.index(\\"amount\\")\\n\\n            getcontext().prec = 1000000\\n            rows = 0\\n            total = Decimal(0)\\n\\n            for row in reader:\\n                if None in row:\\n                    print(\\"Error: invalid row\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                values = list(row.values())\\n                raw = values[amount_index]\\n                if raw is None:\\n                    print(\\"Error: missing amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                text = raw.strip()\\n                if text == \\"\\":\\n                    print(\\"Error: empty amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                try:\\n                    value = Decimal(text)\\n                except InvalidOperation:\\n                    print(\\"Error: invalid amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                if not value.is_finite():\\n                    print(\\"Error: non-finite amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                rows += 1\\n                total += value\\n\\n            if rows == 0:\\n                total = Decimal(0)\\n\\n            if rows == 0:\\n                total_str = \\"0\\"\\n            else:\\n                total_str = format(total, \\"f\\")\\n\\n            result = {\\"rows\\": rows, \\"total\\": total_str}\\n            sys.stdout.write(json.dumps(result, separators=(\\",\\", \\":\\")))\\n            sys.stdout.write(\\"\\\\n\\")\\n\\n    except OSError as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except Exception as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n\\nif __name__ == \\"__main__\\":\\n    main()\\n","README.md":"Usage: python summary.py input.csv\\n\\nReads a UTF-8 BOM CSV file with an amount column and outputs one JSON object:\\n{\\"rows\\": <integer>, \\"total\\": \\"<exact decimal string>\\"}\\n\\n- Decimal scale is preserved: 0.1 + 0.2 = 0.3; 10 - 3 + 2.50 = 9.50.\\n- Numbers exceeding 28 digits are supported without rounding.\\n- Empty data yields {\\"rows\\":0,\\"total\\":\\"0\\"}.\\n- Invalid amount, NaN/Infinity, missing amount column, or I/O error exits with code 2, writes only to stderr, and produces no stdout.\\n- No networking, third-party dependencies, subprocesses, credentials, or PASS markers."}}', 'UPDATE': '{"files":{"summary.py":"import csv\\nimport sys\\nimport json\\nfrom decimal import Decimal, InvalidOperation, getcontext\\n\\n\\ndef main():\\n    if len(sys.argv) != 2:\\n        print(\\"Usage: python summary.py input.csv\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n    path = sys.argv[1]\\n\\n    try:\\n        with open(path, \\"r\\", encoding=\\"utf-8-sig\\", newline=\\"\\") as f:\\n            reader = csv.DictReader(f)\\n            if reader.fieldnames is None:\\n                print(\\"Error: missing header\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            fieldnames = [name.strip() if name is not None else name for name in reader.fieldnames]\\n            if \\"amount\\" not in fieldnames:\\n                print(\\"Error: missing amount column\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            amount_index = fieldnames.index(\\"amount\\")\\n            category_index = fieldnames.index(\\"category\\") if \\"category\\" in fieldnames else None\\n\\n            getcontext().prec = 1000000\\n            rows = 0\\n            total = Decimal(0)\\n            categories = {}\\n\\n            for row in reader:\\n                if None in row:\\n                    print(\\"Error: invalid row\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                values = list(row.values())\\n                raw = values[amount_index]\\n                if raw is None:\\n                    print(\\"Error: missing amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                text = raw.strip()\\n                if text == \\"\\":\\n                    print(\\"Error: empty amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                try:\\n                    value = Decimal(text)\\n                except InvalidOperation:\\n                    print(\\"Error: invalid amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                if not value.is_finite():\\n                    print(\\"Error: non-finite amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                if category_index is None:\\n                    category = \\"\\"\\n                else:\\n                    category_raw = values[category_index]\\n                    if category_raw is None:\\n                        category = \\"\\"\\n                    else:\\n                        category = category_raw.strip()\\n\\n                rows += 1\\n                total += value\\n\\n                if category not in categories:\\n                    categories[category] = Decimal(0)\\n                categories[category] += value\\n\\n            if rows == 0:\\n                total = Decimal(0)\\n\\n            if rows == 0:\\n                total_str = \\"0\\"\\n            else:\\n                total_str = format(total, \\"f\\")\\n\\n            categories_out = {}\\n            for key in sorted(categories.keys()):\\n                categories_out[key] = format(categories[key], \\"f\\")\\n\\n            result = {\\"rows\\": rows, \\"total\\": total_str, \\"categories\\": categories_out}\\n            sys.stdout.write(json.dumps(result, separators=(\\",\\", \\":\\")))\\n            sys.stdout.write(\\"\\\\n\\")\\n\\n    except OSError as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except Exception as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n\\nif __name__ == \\"__main__\\":\\n    main()\\n","README.md":"Usage: python summary.py input.csv\\n\\nReads a UTF-8 BOM CSV file with an amount column and outputs one JSON object:\\n{\\"rows\\": <integer>, \\"total\\": \\"<exact decimal string>\\", \\"categories\\": {\\"<category>\\": \\"<exact decimal string>\\"}}\\n\\n- Decimal scale is preserved: 0.1 + 0.2 = 0.3; 10 - 3 + 2.50 = 9.50.\\n- Numbers exceeding 28 digits are supported without rounding.\\n- Categories are read from the category column, using the empty string if the column is absent or the value is empty.\\n- Category totals are exact Decimal strings and the categories mapping is sorted by category key.\\n- Empty data yields {\\"rows\\":0,\\"total\\":\\"0\\",\\"categories\\":{}}.\\n- Example: amount,category rows 10,A / -3,B / 2.50,A yields total=\\"9.50\\", rows=3, categories={\\"A\\":\\"12.50\\",\\"B\\":\\"-3\\"}.\\n- Invalid amount, NaN/Infinity, missing amount column, or I/O error exits with code 2, writes only to stderr, and produces no stdout.\\n- No networking, third-party dependencies, subprocesses, credentials, or PASS markers."}}'}

def web_capture_cycle(root,image):
    import subprocess,sys
    from executor import canonical
    output=Path(root).resolve();output.mkdir(parents=True,exist_ok=True)
    state=output/'state';records=[];receipts=[];last=None
    started=time.monotonic()
    def send(request):
        p=subprocess.run([sys.executable,'-I',str(Path(__file__).parent/'worker.py'),
                          '--root',str(state),'--image',image],input=canonical(request),capture_output=True,timeout=130)
        if p.returncode:raise Fault('WORKER_INTERFACE_FAILED')
        r=json.loads(p.stdout);records.append({k:v for k,v in r.items() if k!='artifact_base64'})
        (output/'requests.json').write_text(json.dumps(records,indent=2))
        return r
    specs={}
    for mode in ['CREATE','UPDATE']:
        raw=WEB_CAPTURES[mode]
        def captured(provider,model,text,deadline):
            return {'content':raw,'provider':'deepseek-web','model':'WEB_MODEL_ID_UNOBSERVED',
                    'response_id':None,'provenance':'CAPTURED_REAL_WEB_RESPONSE','usage':{},'cost_usd_observed':None}
        spec={'project_id':'deepseek-web-csv','task_id':mode.lower()+'-1','mode':mode,
              'oracle_version':'csv-v1' if mode=='CREATE' else 'csv-v2','deadline_seconds':120}
        if last:spec['base_sha256']=last['sha256']
        specs[mode]=spec
        prepared=Adapter(output/'adapter-state',captured).prepare(spec,[('deepseek-web','WEB_MODEL_ID_UNOBSERVED')])
        result=send({'action':'SUBMIT','task':prepared['task']})
        if result.get('status')!='VERIFIED_ARTIFACT':raise Fault('WEB_CAPTURE_BEHAVIOR_FAILED:'+json.dumps(result))
        receipts.append({'mode':mode,'model_content_sha256':prepared['raw_model_sha256'],
                         'task_sha256':prepared['task_sha256'],'provider_receipt':prepared['receipt'],
                         'executor':{k:v for k,v in result.items() if k!='artifact_base64'}})
        last=result
    def no_generation(*args):raise Fault('REPLAY_CALLED_GENERATOR')
    frozen=Adapter(output/'adapter-state',no_generation).prepare(specs['UPDATE'],[])
    assert frozen['reused']
    repeated=send({'action':'SUBMIT','task':frozen['task']});assert repeated['reused'] and repeated['sha256']==last['sha256']
    query={'action':'FETCH','project_id':'deepseek-web-csv','operation_id':last['operation_id']}
    a=send(query);b=send(query);data=base64.b64decode(a['artifact_base64'])
    assert data==base64.b64decode(b['artifact_base64']) and sha(data)==last['sha256']
    (output/'product.zip').write_bytes(data)
    result={'status':'CAPTURED_MODEL_ISOLATED_HOST_CYCLE','remote_proven':False,'llm_cycle_pass':False,
            'product_sha256':sha(data),'create':receipts[0]['executor'],'update':receipts[1]['executor'],
            'receipts':receipts,'adapter_reused_after_restart':True,'executor_update_reused':True,
            'fetched_twice_equal':True,'elapsed_seconds':time.monotonic()-started,
            'direct_model_api_calls':0,'provider_usage_unobserved':True,
            'provider_context':{k:os.environ.get(k) for k in ['GITHUB_RUN_ID','GITHUB_JOB','GITHUB_SHA','GITHUB_REPOSITORY']}}
    (output/'remote-cycle.json').write_text(json.dumps(result,indent=2));return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--image',required=True)
    p.add_argument('--models');p.add_argument('--web-capture',action='store_true');a=p.parse_args()
    try:
        if a.web_capture:r=web_capture_cycle(a.root,a.image)
        elif a.models:r=cycle(a.root,a.image,json.loads(Path(a.models).read_text()))
        else:raise Fault('MODELS_CONFIG_REQUIRED')
    except (Fault,ValueError,OSError) as e:r={'status':'FAILED','reason':str(e),'llm_cycle_pass':False}
    print(json.dumps({k:v for k,v in r.items() if k not in ['receipts','create','update']}))
    raise SystemExit(0 if r['status'] in ('LIVE_MODEL_ISOLATED_CYCLE','CAPTURED_MODEL_ISOLATED_HOST_CYCLE') else 2)
