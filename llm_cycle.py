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
    import signal
    from model_adapter import http_complete, ProviderFault
    output=Path(root).resolve();output.mkdir(parents=True,exist_ok=True)
    if models!={'deepseek':'deepseek-flash','openrouter':'qwen/qwen3-coder'}:raise Fault('MODELS_CONFIG_INVALID')
    missing=[n for n,(_,env) in ENDPOINTS.items() if not os.environ.get(env)]
    if missing:return {'status':'BLOCKED','reason':'PROVIDER_CREDENTIAL_MISSING','providers':missing,'model_calls':0,'llm_cycle_pass':False}
    calls=[];injections=[];balance_checks=[];balance_delta=None;current_mode=None;started=time.monotonic()
    audit_path=output/'api-audit.json'
    def save_audit():
        audit_path.write_text(json.dumps({'real_post_attempts':len(calls),'max_post_attempts':3,'max_input_utf8_bytes':8192,'input_token_budget_estimated_upper':8320,'max_output_tokens_per_post':2048,'global_deadline_seconds':480,'calls':calls,'controlled_injections':injections,'balance_get_attempts':len(balance_checks),'balance_checks':balance_checks,'deepseek_account_balance_delta':balance_delta},indent=2))
    def balance(stage):
        import urllib.request,urllib.error
        from decimal import Decimal
        from model_adapter import NoRedirect
        record={'stage':stage,'utc':time.time(),'status':'OUTCOME_UNKNOWN'};balance_checks.append(record)
        try:
            key=os.environ['DEEPSEEK_API_KEY']
            req=urllib.request.Request('https://api.deepseek.com/user/balance',headers={'Authorization':'Bearer '+key},method='GET')
            with urllib.request.build_opener(NoRedirect()).open(req,timeout=10) as response:
                raw=response.read(65537)
            if len(raw)>65536:raise ValueError('balance response too large')
            value=json.loads(raw)
            values={r['currency']:Decimal(r['total_balance']) for r in value['balance_infos'] if r['currency'] in ['USD','CNY']}
            if not values or any(not v.is_finite() for v in values.values()):raise ValueError('invalid balance')
            record['status']='OK';return values
        except urllib.error.HTTPError as e:
            record['status']='HTTP_'+str(e.code);return None
        except (urllib.error.URLError,TimeoutError,OSError,ValueError,KeyError,TypeError):
            record['status']='UNOBSERVED';return None
        finally:save_audit()
    def transport(provider,model,text,deadline):
        if len(calls)>=3:raise Fault('API_CALL_LIMIT')
        if len(text.encode())>8192:raise Fault('INPUT_LIMIT')
        for _,env in ENDPOINTS.values():
            if os.environ.get(env) and os.environ[env] in text:raise Fault('PROMPT_SECRET_REJECTED')
        record={'provider':provider,'requested_model':model,'mode':current_mode,'started_utc':time.time(),'status':'OUTCOME_UNKNOWN','input_bytes':len(text.encode())}
        calls.append(record);save_audit()
        try:
            response=http_complete(provider,model,text,deadline,audit=lambda safe:record.update(provider_receipt=safe))
            record['status']='RESPONSE_ACCEPTED';return response
        except ProviderFault as e:
            record['status']=str(e);raise
        finally:
            record['ended_utc']=time.time();save_audit()
    def timeout(*unused):raise Fault('GLOBAL_CYCLE_TIMEOUT')
    signal.signal(signal.SIGALRM,timeout);signal.alarm(480)
    before=balance('before')
    try:
        if balance_checks[-1]['status']=='HTTP_401':raise Fault('DEEPSEEK_AUTH_FAILED_HTTP_401')
        adapter=Adapter(output/'controller-private',transport);state=output/'executor-state'
        backend=PodmanBackend(image,sha(str(state).encode()));executor=Executor(state,backend)
        receipts=[];c=None
        for mode in ['CREATE','UPDATE']:
            current_mode=mode
            spec={'project_id':'llm-csv-pilot','task_id':mode.lower()+'-1','mode':mode,'oracle_version':'csv-v1' if mode=='CREATE' else 'csv-v2','deadline_seconds':120}
            if c:
                files=executor._archive(state/'products'/(c['operation_id']+'.zip'),c['sha256'])
                spec.update(base_sha256=c['sha256'],baseline_files={k:v.decode() for k,v in files.items()})
            prepared=adapter.prepare(spec,[(n,models[n]) for n in ['deepseek','openrouter']])
            if 'task' not in prepared:return {**prepared,'llm_cycle_pass':False}
            if prepared['receipt'].get('provenance')!='LIVE_PROVIDER_API':raise Fault('REAL_API_PROVENANCE_REQUIRED')
            r=executor.submit(prepared['task'])
            receipts.append({'mode':mode,'task_sha256':prepared['task_sha256'],'raw_model_sha256':prepared['raw_model_sha256'],'prompt_sha256':prepared['prompt_sha256'],'provider_receipt':prepared['receipt'],'attempts':prepared['attempts'],'executor':{k:v for k,v in r.items() if k!='artifact_base64'}})
            (output/'receipts.json').write_text(json.dumps(receipts,indent=2))
            if r.get('status')!='VERIFIED_ARTIFACT':return {'status':'FAILED','reason':r.get('reason'),'llm_cycle_pass':False}
            c=r
        # A fresh adapter may replay frozen preparation, never call an API again.
        def forbidden(*unused):raise Fault('REPLAY_CALLED_MODEL')
        replay=Adapter(output/'controller-private',forbidden).prepare(spec,[(n,models[n]) for n in ['deepseek','openrouter']]);assert replay['reused']
        a=executor.fetch('llm-csv-pilot',c['operation_id']);b=executor.fetch('llm-csv-pilot',c['operation_id'])
        data=base64.b64decode(a['artifact_base64']);assert data==base64.b64decode(b['artifact_base64']) and sha(data)==c['sha256']
        repeated=executor.submit(prepared['task']);assert repeated['reused'] and repeated['sha256']==c['sha256']
        (output/'product.zip').write_bytes(data)
        result={'status':'LIVE_MODEL_ISOLATED_CYCLE','remote_provider_verified_externally':False,'llm_cycle_pass':False,'receipts':receipts,'downloaded_twice_equal':True,'update_reused':True,'adapter_restart_reused':True,'product_sha256':sha(data),'elapsed_seconds':time.monotonic()-started,'real_post_attempts':len(calls),'controlled_failover':False,'actual_provider_outage_observed':False,'provider_context':{n:os.environ.get(n) for n in ['GITHUB_RUN_ID','GITHUB_JOB','GITHUB_SHA','GITHUB_REPOSITORY']}}
        (output/'remote-cycle.json').write_text(json.dumps(result,indent=2));return result
    finally:
        after=balance('after') if calls else None
        if before is not None and after is not None and set(before)==set(after):balance_delta={n:str(before[n]-after[n]) for n in before}
        signal.alarm(0);save_audit()


def repair(root,image,failed,feedback):
    import sqlite3,shutil,signal
    from executor import canonical,normalize
    from model_adapter import http_complete,candidate,SECRET,prompt
    from contracts import CASES
    output=Path(root).resolve();output.mkdir(parents=True,exist_ok=True)
    failed=Path(failed);feedback=json.loads(Path(feedback).read_text())
    db=sqlite3.connect('file:'+str((failed/'controller-private/adapter.sqlite3').resolve())+'?mode=ro',uri=True)
    old=json.loads(db.execute("SELECT result FROM jobs WHERE project='llm-csv-pilot' AND taskid='create-1' AND phase='PREPARED'").fetchone()[0]);db.close()
    original=old['raw_model_content'];original_sha=sha(original.encode())
    if original_sha!='e7dae265cb404b1921c66c4497e5c33b53afc3da27e5fa5082c88f335975622c' or feedback['raw_model_sha256']!=original_sha:raise Fault('REPAIR_ORIGIN_MISMATCH')
    assert feedback['expected']['total']=='123456789012345678901234567890.1' and feedback['actual']['total']=='123456789012345678901234567900'
    if not os.environ.get('DEEPSEEK_API_KEY'):raise Fault('DEEPSEEK_KEY_MISSING')
    text=('Repair the EXISTING failed implementation below; this is the same project, not a new CREATE from scratch. '
          'Determine the cause yourself using the independent behavioral feedback and correct the existing code, preserving every other requirement. '
          'Unchanged contract: Python stdlib summary.py; python summary.py input.csv; UTF-8 BOM CSV with amount column; '
          'JSON rows integer and exact Decimal total string, preserving scale (0.1+0.2=0.3; 10-3+2.50=9.50); support numbers exceeding 28 digits without rounding. '
          'Empty data rows=0,total="0". Invalid amount, NaN/Infinity, missing amount column or I/O error: exit2, empty stdout, stderr diagnostic. '
          'No network, subprocesses, dependencies, credentials or PASS markers. '
          'Return only one JSON object with key files, containing COMPLETE corrected executable summary.py and actual README.md. '
          'No placeholder labels, fences or explanations.\nINDEPENDENT ORACLE INPUT:\n'+CASES[6][0]+
          '\nEXPECTED:\n'+json.dumps(feedback['expected'])+'\nACTUAL:\n'+json.dumps(feedback['actual'])+
          '\nEXISTING FAILED FILES:\n'+original)
    if len(text.encode())>8192 or SECRET.search(text):raise Fault('REPAIR_PROMPT_REJECTED')
    (output/'sent-prompt.txt').write_text(text)
    audit={'real_post_attempts':0,'max_post_attempts':1,'max_input_utf8_bytes':8192,'max_output_tokens':2048,'deadline_seconds':480,'original_model_sha256':original_sha,'prompt_sha256':sha(text.encode()),'input_bytes':len(text.encode()),'provider':'deepseek','started_utc':time.time(),'status':'NOT_SENT'}
    def save(): (output/'repair-api-audit.json').write_text(json.dumps(audit,indent=2))
    save()
    def timeout(*unused):raise Fault('GLOBAL_REPAIR_TIMEOUT')
    signal.signal(signal.SIGALRM,timeout);signal.alarm(480)
    try:
        audit.update(real_post_attempts=1,status='OUTCOME_UNKNOWN');save()
        response=http_complete('deepseek','deepseek-flash',text,Deadline(60),audit=lambda safe:audit.update(provider_receipt=safe))
        audit.update(status='RESPONSE_ACCEPTED',ended_utc=time.time());save()
        files=candidate(response['content']);assert set(files)==set(old['task']['files'])
        (output/'model-response.json').write_text(response['content'])
        state=output/'state';shutil.copytree(failed/'executor-state',state)
        backend=PodmanBackend(image,sha(str(state).encode()));executor=Executor(state,backend)
        assert executor.head(old['task']['project_id']) is None
        # Preserve project and failed history; changed payload requires a distinct repair attempt identity.
        task=normalize({**old['task'],'task_id':'create-1-repair-1','files':files})
        (output/'repair-task.json').write_bytes(canonical(task))
        r=executor.submit(task)
        result={'status':'MODEL_REPAIR_CHECKED_ON_WORKER','qualification_pass':False,'original_task_sha256':sha(canonical(old['task'])),'original_model_sha256':original_sha,'repair_model_sha256':sha(response['content'].encode()),'repair_task_sha256':sha(canonical(task)),'provider_receipt':{k:v for k,v in response.items() if k!='content'},'executor':{k:v for k,v in r.items() if k!='artifact_base64'},'provider_context':{n:os.environ.get(n) for n in ['GITHUB_RUN_ID','GITHUB_JOB','GITHUB_SHA','GITHUB_REPOSITORY']},'update_called':False}
        (output/'repair-evidence.json').write_text(json.dumps(result,indent=2))
        if r.get('status')!='VERIFIED_ARTIFACT':raise Fault('REPAIRED_CREATE_BEHAVIOR_FAILED')
        repeated=executor.submit(task);assert repeated['reused'] and repeated['sha256']==r['sha256']
        a=executor.fetch(task['project_id'],r['operation_id']);b=executor.fetch(task['project_id'],r['operation_id'])
        data=base64.b64decode(a['artifact_base64']);assert data==base64.b64decode(b['artifact_base64']) and sha(data)==r['sha256']
        (output/'product.zip').write_bytes(data)
        result.update(product_sha256=r['sha256'],executor_reused=True,fetched_twice_equal=True)
        (output/'repair-evidence.json').write_text(json.dumps(result,indent=2))
        # No UPDATE generation is authorized here. Prepare only the unchanged next contract.
        spec={'project_id':task['project_id'],'task_id':'update-1','mode':'UPDATE','oracle_version':'csv-v2','deadline_seconds':120,'base_sha256':r['sha256'],'baseline_files':files}
        update_prompt=prompt(spec)
        (output/'update-plan.json').write_text(json.dumps({'status':'PENDING_INDEPENDENT_CREATE_ACCEPTANCE_AND_UPDATE_SPEND_AUTHORIZATION','spec':spec,'prompt_bytes':len(update_prompt.encode()),'additional_model_calls_required':1,'max_output_tokens':2048,'proposed_budget_usd':0.01,'requires_controller_pass':True},indent=2))
        return result
    except Fault as e:
        audit['terminal_reason']=str(e);raise
    finally:
        signal.alarm(0);audit['ended_utc']=time.time();save()

# Exact synthetic JSON code blocks copied from authenticated DeepSeek web UI.
# Replaying these bytes is not an API call; external provenance must be checked.
WEB_CAPTURES = {'CREATE': '{"files":{"summary.py":"import csv\\nimport sys\\nimport json\\nfrom decimal import Decimal, InvalidOperation, getcontext\\n\\n\\ndef main():\\n    if len(sys.argv) != 2:\\n        print(\\"Usage: python summary.py input.csv\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n    path = sys.argv[1]\\n\\n    try:\\n        with open(path, \\"r\\", encoding=\\"utf-8-sig\\", newline=\\"\\") as f:\\n            reader = csv.DictReader(f)\\n            if reader.fieldnames is None:\\n                print(\\"Error: missing header\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            fieldnames = [name.strip() if name is not None else name for name in reader.fieldnames]\\n            if \\"amount\\" not in fieldnames:\\n                print(\\"Error: missing amount column\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            amount_index = fieldnames.index(\\"amount\\")\\n\\n            getcontext().prec = 1000000\\n            rows = 0\\n            total = Decimal(0)\\n\\n            for row in reader:\\n                if None in row:\\n                    print(\\"Error: invalid row\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                values = list(row.values())\\n                raw = values[amount_index]\\n                if raw is None:\\n                    print(\\"Error: missing amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                text = raw.strip()\\n                if text == \\"\\":\\n                    print(\\"Error: empty amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                try:\\n                    value = Decimal(text)\\n                except InvalidOperation:\\n                    print(\\"Error: invalid amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                if not value.is_finite():\\n                    print(\\"Error: non-finite amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                rows += 1\\n                total += value\\n\\n            if rows == 0:\\n                total = Decimal(0)\\n\\n            if rows == 0:\\n                total_str = \\"0\\"\\n            else:\\n                total_str = format(total, \\"f\\")\\n\\n            result = {\\"rows\\": rows, \\"total\\": total_str}\\n            sys.stdout.write(json.dumps(result, separators=(\\",\\", \\":\\")))\\n            sys.stdout.write(\\"\\\\n\\")\\n\\n    except OSError as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except Exception as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n\\nif __name__ == \\"__main__\\":\\n    main()\\n","README.md":"Usage: python summary.py input.csv\\n\\nReads a UTF-8 BOM CSV file with an amount column and outputs one JSON object:\\n{\\"rows\\": <integer>, \\"total\\": \\"<exact decimal string>\\"}\\n\\n- Decimal scale is preserved: 0.1 + 0.2 = 0.3; 10 - 3 + 2.50 = 9.50.\\n- Numbers exceeding 28 digits are supported without rounding.\\n- Empty data yields {\\"rows\\":0,\\"total\\":\\"0\\"}.\\n- Invalid amount, NaN/Infinity, missing amount column, or I/O error exits with code 2, writes only to stderr, and produces no stdout.\\n- No networking, third-party dependencies, subprocesses, credentials, or PASS markers."}}', 'UPDATE': '{"files":{"summary.py":"import csv\\nimport sys\\nimport json\\nfrom decimal import Decimal, InvalidOperation, getcontext\\n\\n\\ndef main():\\n    if len(sys.argv) != 2:\\n        print(\\"Usage: python summary.py input.csv\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n    path = sys.argv[1]\\n\\n    try:\\n        with open(path, \\"r\\", encoding=\\"utf-8-sig\\", newline=\\"\\") as f:\\n            reader = csv.DictReader(f)\\n            if reader.fieldnames is None:\\n                print(\\"Error: missing header\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            fieldnames = [name.strip() if name is not None else name for name in reader.fieldnames]\\n            if \\"amount\\" not in fieldnames:\\n                print(\\"Error: missing amount column\\", file=sys.stderr)\\n                sys.exit(2)\\n\\n            amount_index = fieldnames.index(\\"amount\\")\\n            category_index = fieldnames.index(\\"category\\") if \\"category\\" in fieldnames else None\\n\\n            getcontext().prec = 1000000\\n            rows = 0\\n            total = Decimal(0)\\n            categories = {}\\n\\n            for row in reader:\\n                if None in row:\\n                    print(\\"Error: invalid row\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                values = list(row.values())\\n                raw = values[amount_index]\\n                if raw is None:\\n                    print(\\"Error: missing amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                text = raw.strip()\\n                if text == \\"\\":\\n                    print(\\"Error: empty amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                try:\\n                    value = Decimal(text)\\n                except InvalidOperation:\\n                    print(\\"Error: invalid amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                if not value.is_finite():\\n                    print(\\"Error: non-finite amount\\", file=sys.stderr)\\n                    sys.exit(2)\\n\\n                if category_index is None:\\n                    category = \\"\\"\\n                else:\\n                    category_raw = values[category_index]\\n                    if category_raw is None:\\n                        category = \\"\\"\\n                    else:\\n                        category = category_raw.strip()\\n\\n                rows += 1\\n                total += value\\n\\n                if category not in categories:\\n                    categories[category] = Decimal(0)\\n                categories[category] += value\\n\\n            if rows == 0:\\n                total = Decimal(0)\\n\\n            if rows == 0:\\n                total_str = \\"0\\"\\n            else:\\n                total_str = format(total, \\"f\\")\\n\\n            categories_out = {}\\n            for key in sorted(categories.keys()):\\n                categories_out[key] = format(categories[key], \\"f\\")\\n\\n            result = {\\"rows\\": rows, \\"total\\": total_str, \\"categories\\": categories_out}\\n            sys.stdout.write(json.dumps(result, separators=(\\",\\", \\":\\")))\\n            sys.stdout.write(\\"\\\\n\\")\\n\\n    except OSError as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except Exception as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n\\n\\nif __name__ == \\"__main__\\":\\n    main()\\n","README.md":"Usage: python summary.py input.csv\\n\\nReads a UTF-8 BOM CSV file with an amount column and outputs one JSON object:\\n{\\"rows\\": <integer>, \\"total\\": \\"<exact decimal string>\\", \\"categories\\": {\\"<category>\\": \\"<exact decimal string>\\"}}\\n\\n- Decimal scale is preserved: 0.1 + 0.2 = 0.3; 10 - 3 + 2.50 = 9.50.\\n- Numbers exceeding 28 digits are supported without rounding.\\n- Categories are read from the category column, using the empty string if the column is absent or the value is empty.\\n- Category totals are exact Decimal strings and the categories mapping is sorted by category key.\\n- Empty data yields {\\"rows\\":0,\\"total\\":\\"0\\",\\"categories\\":{}}.\\n- Example: amount,category rows 10,A / -3,B / 2.50,A yields total=\\"9.50\\", rows=3, categories={\\"A\\":\\"12.50\\",\\"B\\":\\"-3\\"}.\\n- Invalid amount, NaN/Infinity, missing amount column, or I/O error exits with code 2, writes only to stderr, and produces no stdout.\\n- No networking, third-party dependencies, subprocesses, credentials, or PASS markers."}}'}

def web_capture_cycle(root,image,capture_provider="deepseek-web"):
    captures=QWEN_WEB_CAPTURES if capture_provider=="qwen-web" else WEB_CAPTURES
    model_id="Qwen3.7-Plus" if capture_provider=="qwen-web" else "WEB_MODEL_ID_UNOBSERVED"
    project="qwen-web-csv" if capture_provider=="qwen-web" else "deepseek-web-csv"
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
        raw=captures[mode]
        def captured(provider,model,text,deadline):
            value=DEEPSEEK_CONTROLLED_REFUSAL if provider=='deepseek-controlled-refusal' else raw
            return {'content':value,'provider':provider,'model':model,
                    'response_id':None,'provenance':'CAPTURED_REAL_WEB_RESPONSE','usage':{},'cost_usd_observed':None}
        spec={'project_id':project,'task_id':mode.lower()+'-1','mode':mode,
              'oracle_version':'csv-v1' if mode=='CREATE' else 'csv-v2','deadline_seconds':120}
        if last:spec['base_sha256']=last['sha256']
        specs[mode]=spec
        order=[(capture_provider,model_id)]
        if capture_provider=='qwen-web' and mode=='CREATE':
            order=[('deepseek-controlled-refusal','WEB_MODEL_ID_UNOBSERVED'),*order]
        prepared=Adapter(output/'adapter-state',captured).prepare(spec,order)
        if mode=='CREATE' and capture_provider=='qwen-web':
            assert prepared['attempts']==[{'provider':'deepseek-controlled-refusal','model':'WEB_MODEL_ID_UNOBSERVED','status':'MODEL_SCHEMA_INVALID'}]
        result=send({'action':'SUBMIT','task':prepared['task']})
        if result.get('status')!='VERIFIED_ARTIFACT':raise Fault('WEB_CAPTURE_BEHAVIOR_FAILED:'+json.dumps(result))
        receipts.append({'mode':mode,'model_content_sha256':prepared['raw_model_sha256'],
                         'task_sha256':prepared['task_sha256'],'attempts':prepared['attempts'],'provider_receipt':prepared['receipt'],
                         'executor':{k:v for k,v in result.items() if k!='artifact_base64'}})
        last=result
    def no_generation(*args):raise Fault('REPLAY_CALLED_GENERATOR')
    frozen=Adapter(output/'adapter-state',no_generation).prepare(specs['UPDATE'],[])
    assert frozen['reused']
    repeated=send({'action':'SUBMIT','task':frozen['task']});assert repeated['reused'] and repeated['sha256']==last['sha256']
    query={'action':'FETCH','project_id':project,'operation_id':last['operation_id']}
    a=send(query);b=send(query);data=base64.b64decode(a['artifact_base64'])
    assert data==base64.b64decode(b['artifact_base64']) and sha(data)==last['sha256']
    (output/'product.zip').write_bytes(data)
    result={'status':'CAPTURED_MODEL_ISOLATED_HOST_CYCLE','remote_proven':False,'llm_cycle_pass':False,
            'product_sha256':sha(data),'create':receipts[0]['executor'],'update':receipts[1]['executor'],
            'receipts':receipts,'adapter_reused_after_restart':True,'executor_update_reused':True,
            'fetched_twice_equal':True,'elapsed_seconds':time.monotonic()-started,
            'direct_model_api_calls':0,'provider_usage_unobserved':True,
            'selected_capture_provider':capture_provider,'controlled_refusal_injected':capture_provider=='qwen-web',
            'controlled_refusal_sha256':sha(DEEPSEEK_CONTROLLED_REFUSAL.encode()) if capture_provider=='qwen-web' else None,
            'actual_service_outage_observed':False,
            'provider_context':{k:os.environ.get(k) for k in ['GITHUB_RUN_ID','GITHUB_JOB','GITHUB_SHA','GITHUB_REPOSITORY']}}
    (output/'remote-cycle.json').write_text(json.dumps(result,indent=2));return result

# Actual deliberately injected negative reply observed in DeepSeek Web.
DEEPSEEK_CONTROLLED_REFUSAL = '{"unsupported":true}'
QWEN_WEB_CAPTURES = {'CREATE': '{\n  "files": {\n    "summary.py": "#!/usr/bin/env python3\\nimport sys\\nimport csv\\nimport json\\nfrom decimal import Decimal, InvalidOperation, getcontext\\n\\ndef main():\\n    if len(sys.argv) != 2:\\n        print(\\"Usage: python summary.py input.csv\\", file=sys.stderr)\\n        sys.exit(2)\\n    \\n    filename = sys.argv[1]\\n    getcontext().prec = 1000\\n    \\n    try:\\n        with open(filename, \'r\', encoding=\'utf-8-sig\', newline=\'\') as f:\\n            reader = csv.DictReader(f)\\n            \\n            if not reader.fieldnames or \'amount\' not in reader.fieldnames:\\n                print(\\"Error: missing \'amount\' column\\", file=sys.stderr)\\n                sys.exit(2)\\n            \\n            total = Decimal(\'0\')\\n            rows = 0\\n            \\n            for row in reader:\\n                amount_str = row.get(\'amount\', \'\')\\n                \\n                if not amount_str or not amount_str.strip():\\n                    print(\\"Error: invalid amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n                \\n                try:\\n                    amount = Decimal(amount_str.strip())\\n                except InvalidOperation:\\n                    print(f\\"Error: invalid amount: {amount_str}\\", file=sys.stderr)\\n                    sys.exit(2)\\n                \\n                if amount.is_nan() or amount.is_infinite():\\n                    print(\\"Error: amount is NaN or Infinity\\", file=sys.stderr)\\n                    sys.exit(2)\\n                \\n                total += amount\\n                rows += 1\\n            \\n            result = {\\"rows\\": rows, \\"total\\": str(total)}\\n            print(json.dumps(result))\\n            \\n    except FileNotFoundError:\\n        print(f\\"Error: file not found: {filename}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except IOError as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except Exception as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n\\nif __name__ == \'__main__\':\\n    main()",\n    "README.md": "# Summary Tool\\n\\nUsage: python summary.py input.csv\\n\\nReads a UTF-8 BOM CSV file with an \'amount\' column and outputs a JSON object with:\\n- rows: number of data rows\\n- total: sum of all amounts as exact decimal string\\n\\nExamples:\\n- 0.1 + 0.2 = 0.3\\n- 10 - 3 + 2.50 = 9.50\\n\\nError handling:\\n- Exit code 2 for invalid amounts, NaN/Infinity, missing amount column, or I/O errors\\n- Diagnostics printed to stderr\\n- No output to stdout on error"\n  }\n}', 'UPDATE': '{\n  "files": {\n    "summary.py": "#!/usr/bin/env python3\\nimport sys\\nimport csv\\nimport json\\nfrom decimal import Decimal, InvalidOperation, getcontext\\n\\ndef main():\\n    if len(sys.argv) != 2:\\n        print(\\"Usage: python summary.py input.csv\\", file=sys.stderr)\\n        sys.exit(2)\\n    \\n    filename = sys.argv[1]\\n    getcontext().prec = 10000\\n    \\n    try:\\n        with open(filename, \'r\', encoding=\'utf-8-sig\', newline=\'\') as f:\\n            reader = csv.DictReader(f)\\n            \\n            if not reader.fieldnames or \'amount\' not in reader.fieldnames:\\n                print(\\"Error: missing \'amount\' column\\", file=sys.stderr)\\n                sys.exit(2)\\n            \\n            has_category = \'category\' in reader.fieldnames\\n            total = Decimal(\'0\')\\n            categories = {}\\n            rows = 0\\n            \\n            for row in reader:\\n                amount_str = row.get(\'amount\', \'\')\\n                \\n                if amount_str is None or not amount_str.strip():\\n                    print(\\"Error: invalid amount value\\", file=sys.stderr)\\n                    sys.exit(2)\\n                \\n                try:\\n                    amount = Decimal(amount_str.strip())\\n                except InvalidOperation:\\n                    print(f\\"Error: invalid amount: {amount_str}\\", file=sys.stderr)\\n                    sys.exit(2)\\n                \\n                if amount.is_nan() or amount.is_infinite():\\n                    print(\\"Error: amount is NaN or Infinity\\", file=sys.stderr)\\n                    sys.exit(2)\\n                \\n                cat = \'\'\\n                if has_category:\\n                    val = row.get(\'category\')\\n                    if val is not None:\\n                        cat = val\\n                \\n                total += amount\\n                if cat in categories:\\n                    categories[cat] += amount\\n                else:\\n                    categories[cat] = amount\\n                rows += 1\\n            \\n            sorted_cats = {k: str(categories[k]) for k in sorted(categories.keys())}\\n            result = {\\n                \\"rows\\": rows,\\n                \\"total\\": str(total),\\n                \\"categories\\": sorted_cats\\n            }\\n            print(json.dumps(result))\\n            \\n    except FileNotFoundError:\\n        print(f\\"Error: file not found: {filename}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except IOError as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n    except Exception as e:\\n        print(f\\"Error: {e}\\", file=sys.stderr)\\n        sys.exit(2)\\n\\nif __name__ == \'__main__\':\\n    main()",\n    "README.md": "# Summary Tool\\n\\nUsage: python summary.py input.csv\\n\\nReads a UTF-8 BOM CSV file with an \'amount\' column and optional \'category\' column.\\nOutputs a JSON object with:\\n- rows: number of data rows\\n- total: sum of all amounts as exact decimal string\\n- categories: sorted mapping of category to exact decimal total (empty string key if category column absent)\\n\\nExamples:\\n- 0.1 + 0.2 = 0.3\\n- 10 - 3 + 2.50 = 9.50\\n- Categories: {\\"A\\": \\"12.50\\", \\"B\\": \\"-3\\"}\\n\\nError handling:\\n- Exit code 2 for invalid amounts, NaN/Infinity, missing amount column, or I/O errors\\n- Diagnostics printed to stderr\\n- No output to stdout on error"\n  }\n}'}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--image',required=True)
    p.add_argument('--repair-from');p.add_argument('--feedback-from');p.add_argument('--models');p.add_argument('--web-capture',action='store_true');p.add_argument('--qwen-capture',action='store_true');a=p.parse_args()
    try:
        if a.repair_from:r=repair(a.root,a.image,a.repair_from,a.feedback_from)
        elif a.qwen_capture:r=web_capture_cycle(a.root,a.image,'qwen-web')
        elif a.web_capture:r=web_capture_cycle(a.root,a.image)
        elif a.models:r=cycle(a.root,a.image,json.loads(Path(a.models).read_text()))
        else:raise Fault('MODELS_CONFIG_REQUIRED')
    except (Fault,ValueError,OSError) as e:r={'status':'FAILED','reason':str(e),'llm_cycle_pass':False}
    print(json.dumps({k:v for k,v in r.items() if k not in ['receipts','create','update']}))
    raise SystemExit(0 if r['status'] in ('LIVE_MODEL_ISOLATED_CYCLE','CAPTURED_MODEL_ISOLATED_HOST_CYCLE','MODEL_REPAIR_CHECKED_ON_WORKER') else 2)
