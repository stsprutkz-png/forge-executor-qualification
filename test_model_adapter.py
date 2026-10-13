import json
from pathlib import Path
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from executor import Executor
from model_adapter import Adapter, candidate, http_complete, ProviderFault
from processes import Fault, Deadline
from sample_tasks import SOURCE_V1, SOURCE_V2
from test_fixtures import FixtureBackend

def spec(mode='CREATE',base=None):
    r={'project_id':'llm-synthetic','task_id':'create' if mode=='CREATE' else 'update','mode':mode,
       'oracle_version':'csv-v1' if mode=='CREATE' else 'csv-v2','deadline_seconds':30}
    if base:r.update(base_sha256=base,baseline_files={'summary.py':SOURCE_V1})
    return r

class Tests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
    def tearDown(self):self.tmp.cleanup()
    def mock(self,source=SOURCE_V1):
        self.calls=[]
        def call(provider,model,text,deadline):
            self.calls.append(provider)
            if provider=='deepseek':raise ProviderFault('PROVIDER_HTTP_503')
            return {'content':json.dumps({'files':{'summary.py':source}}),'provider':provider,'model':model,
                    'provenance':'TEST_DOUBLE_NOT_LLM','usage':{},'cost_usd_observed':None}
        return call
    def test_failover_freeze_repeat_restart(self):
        a=Adapter(self.root/'gen',self.mock());r=a.prepare(spec(),[('deepseek','test'),('openrouter','test')])
        self.assertEqual(self.calls,['deepseek','openrouter']);self.assertEqual(r['receipt']['provenance'],'TEST_DOUBLE_NOT_LLM')
        def never(*args):raise AssertionError('repeat must not call provider')
        reused=Adapter(self.root/'gen',never).prepare(spec(),[])
        self.assertTrue(reused['reused']);self.assertEqual(reused['task_sha256'],r['task_sha256'])
    def test_create_update_fixture_delivery_is_not_llm_or_remote_pass(self):
        backend=FixtureBackend();executor=Executor(self.root/'executor',backend)
        c=Adapter(self.root/'gen',self.mock()).prepare(spec(),[('openrouter','test')]);created=executor.submit(c['task'])
        u=Adapter(self.root/'gen',self.mock(SOURCE_V2)).prepare(spec('UPDATE',created['sha256']),[('openrouter','test')])
        updated=executor.submit(u['task']);calls=backend.calls
        first=executor.fetch('llm-synthetic',updated['operation_id']);second=executor.fetch('llm-synthetic',updated['operation_id'])
        self.assertEqual(first['artifact_base64'],second['artifact_base64']);self.assertEqual(backend.calls,calls)
        self.assertEqual(updated['status'],'VERIFIED_TEST_FIXTURE');self.assertFalse(u['executor_verified'])
    def test_changed_identity_rejected_after_restart(self):
        a=Adapter(self.root,self.mock());a.prepare(spec(),[('openrouter','test')])
        changed=spec();changed['deadline_seconds']=20
        with self.assertRaisesRegex(Fault,'IDENTITY_COLLISION'):a.prepare(changed,[])
    def test_model_cannot_choose_controller_identity_or_contract(self):
        for raw in ['{"files":{"summary.py":"pass"},"project_id":"other"}',
                    '{"files":{"../x":"pass","summary.py":"pass"}}',
                    '{"files":{"summary.py":"pass"},"files":{"summary.py":"pass"}}',
                    '```json\n{"files":{"summary.py":"pass"}}\n```',
                    '{"files":{"summary.py":"sk-123456789012345678901234"}}']:
            with self.assertRaises(ProviderFault):candidate(raw)
    def test_unknown_generation_not_silently_replayed_or_failed_over(self):
        calls=[]
        def broken(*args):calls.append(args[0]);raise ProviderFault('PROVIDER_OUTCOME_UNKNOWN')
        a=Adapter(self.root,broken);r=a.prepare(spec(),[('deepseek','test'),('openrouter','test')])
        self.assertEqual(r['status'],'BLOCKED');self.assertEqual(calls,['deepseek'])
        with self.assertRaisesRegex(Fault,'UNRESOLVED'):Adapter(self.root,broken).prepare(spec(),[])
    def test_no_credentials_never_sends_request(self):
        with patch.dict('os.environ',{},clear=True),patch('urllib.request.build_opener',side_effect=AssertionError('no request')):
            for name in ['deepseek','openrouter']:
                with self.assertRaisesRegex(ProviderFault,'CREDENTIAL_MISSING'):http_complete(name,'test','synthetic',Deadline(1))
    def test_concurrent_generation_happens_once(self):
        a=Adapter(self.root,self.mock())
        with ThreadPoolExecutor(2) as pool:results=list(pool.map(lambda _:a.prepare(spec(),[('openrouter','test')]),range(2)))
        self.assertEqual(self.calls,['openrouter']);self.assertEqual(sum(x['reused'] for x in results),1)
    def test_fake_pass_model_output_fails_executor_behavior(self):
        r=Adapter(self.root/'gen',self.mock("print('PASS')")).prepare(spec(),[('openrouter','test')])
        result=Executor(self.root/'exec',FixtureBackend()).submit(r['task'])
        self.assertEqual(result['status'],'FAILED');self.assertEqual(result['reason'],'BEHAVIOR_FAILED')

    def test_live_transport_schema_usage_and_secret_metadata_with_http_double(self):
        import io
        body={'id':'test-response','model':'test-model','choices':[{'finish_reason':'stop','message':{'content':json.dumps({'files':{'summary.py':SOURCE_V1}})}}],
              'usage':{'prompt_tokens':10,'completion_tokens':20,'total_tokens':30,'cost':0.0001,'unsafe':'not retained'}}
        class Opener:
            def open(self,request,timeout):return io.BytesIO(json.dumps(body).encode())
        with patch.dict('os.environ',{'OPENROUTER_API_KEY':'test-credential-not-a-real-key'}),patch('urllib.request.build_opener',return_value=Opener()):
            r=http_complete('openrouter','test','synthetic',Deadline(1))
            self.assertEqual(r['usage']['total_tokens'],30);self.assertEqual(r['cost_usd_observed'],0.0001);self.assertNotIn('unsafe',r['usage'])
            body['choices'][0]['finish_reason']='length'
            with self.assertRaisesRegex(ProviderFault,'INCOMPLETE'):http_complete('openrouter','test','synthetic',Deadline(1))
            body['choices'][0]['finish_reason']='stop';body['id']='test-credential-not-a-real-key'
            with self.assertRaisesRegex(ProviderFault,'SECRET'):http_complete('openrouter','test','synthetic',Deadline(1))

if __name__=='__main__':unittest.main()

class AuditDiagnosticsTests(unittest.TestCase):
    def invoke(self,content,path,events,finish='stop',transport=None,usage=None):
        from model_adapter import http_complete
        from processes import Deadline
        import json,os
        from unittest.mock import patch
        class Response:
            status=200
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def read(self,*a):return json.dumps({'id':'synthetic-response','model':'deepseek-flash','usage':({'prompt_tokens':7,'completion_tokens':11} if usage is None else usage),'choices':[{'finish_reason':finish,'message':{'content':content}}]}).encode()
        class Fake:
            def open(self,*a,**kw):return Response()
        with patch.dict(os.environ,{'DEEPSEEK_API_KEY':'SYNTHETIC_LOCAL_SECRET_VALUE','OPENROUTER_API_KEY':'OTHER_LOCAL_SECRET_VALUE'}),patch('urllib.request.build_opener',return_value=Fake()):
            return (transport or http_complete)('deepseek','deepseek-flash','synthetic',Deadline(30),events.append,content_path=path)
    def test_valid_snapshot_and_status_order(self):
        from pathlib import Path
        import tempfile,json
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'raw.txt';events=[]
            r=self.invoke(json.dumps({'files':{'summary.py':'# synthetic','README.md':'usage'}}),path,events)
            self.assertEqual(r['schema_status'],'ACCEPTED');self.assertTrue(path.exists())
            self.assertEqual(events[0]['http_status'],'RESPONSE_RECEIVED');self.assertEqual(events[0]['schema_status'],'NOT_RUN')
            self.assertEqual(events[-2]['content_snapshot']['saved_sha256'],__import__('hashlib').sha256(path.read_bytes()).hexdigest())
    def test_specific_schema_errors_saved_before_rejection(self):
        import tempfile,json
        from pathlib import Path
        from model_adapter import ProviderFault
        cases=[('{','json_syntax'),('[]','root_type'),('{"summary.py":"source"}','root_keys'),('{"files":[]}','files_type'),('{"files":{"README.md":"usage"}}','missing_required_files'),('{"files":{"summary.py":9}}','file_content_type'),('{"files":{"summary.py":"x","evil.py":"x"}}','files_outside_contract'),('{"files":{},"files":{}}','duplicate_key'),('{"files":NaN}','nonfinite_json_constant')]
        for content,reason in cases:
            with self.subTest(reason=reason),tempfile.TemporaryDirectory() as tmp:
                path=Path(tmp)/'raw.txt';events=[]
                with self.assertRaises(ProviderFault) as got:self.invoke(content,path,events)
                self.assertEqual(got.exception.detail['violation'],reason);self.assertEqual(path.read_text(),content)
                self.assertEqual(events[-1]['http_status'],'RESPONSE_RECEIVED');self.assertEqual(events[-1]['schema_status'],'REJECTED')
                self.assertEqual(events[-1]['schema_error']['violation'],reason)
    def test_redaction_and_utf8_size_limit(self):
        import tempfile,json
        from pathlib import Path
        from model_adapter import ProviderFault
        secret='SYNTHETIC_LOCAL_SECRET_VALUE';other='OTHER_LOCAL_SECRET_VALUE'
        content=json.dumps({'files':{'summary.py':secret+' '+other+' '+'sk-'+'x'*40+'\n-----BEGIN PRIVATE KEY-----\nPRIVATE_BODY\n-----END PRIVATE KEY-----'}})
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw';events=[]
            with self.assertRaises(ProviderFault):self.invoke(content,p,events)
            saved=p.read_text();self.assertNotIn(secret,saved);self.assertNotIn(other,saved);self.assertNotIn('PRIVATE_BODY',saved);self.assertNotIn('sk-'+'x'*40,saved)
            with self.assertRaises(ProviderFault):self.invoke('я'*40000,p,[])
            self.assertLessEqual(p.stat().st_size,65536);p.read_text(encoding='utf-8')
    def test_completion_rejection_retains_content(self):
        import tempfile
        from pathlib import Path
        from model_adapter import ProviderFault
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw';events=[]
            with self.assertRaisesRegex(ProviderFault,'COMPLETION_INCOMPLETE'):self.invoke('partial',p,events,finish='length')
            self.assertEqual(p.read_text(),'partial');self.assertEqual(events[-1]['schema_error']['violation'],'completion_not_final')
    def test_nonstring_content_records_type(self):
        import tempfile
        from pathlib import Path
        from model_adapter import ProviderFault
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw';events=[]
            with self.assertRaises(ProviderFault) as e:self.invoke(None,p,events)
            self.assertEqual(e.exception.detail['actual_type'],'NoneType');self.assertEqual(p.read_text(),'null')
    def test_snapshot_symlink_is_not_followed(self):
        import tempfile
        from pathlib import Path
        from model_adapter import safe_content_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            target=Path(tmp)/'target';target.write_text('untouched');link=Path(tmp)/'link';link.symlink_to(target)
            with self.assertRaises(OSError):safe_content_snapshot(link,'untrusted')
            self.assertEqual(target.read_text(),'untouched')
    def test_update_rejection_keeps_repaired_head(self):
        import tempfile,os,json,sqlite3,hashlib
        from pathlib import Path
        from unittest.mock import patch
        from llm_cycle import update_existing
        from model_adapter import ProviderFault
        from processes import Fault
        # Synthetic harness artifact contains failed/fixed history and no provider credentials.
        base=Path(os.environ['DIAGNOSTIC_BASELINE_DIR']);acceptance=Path(os.environ['DIAGNOSTIC_ACCEPTANCE_FILE'])
        profile=json.loads((base/'runtime-profile.json').read_text())
        real=self.invoke
        from model_adapter import http_complete as original_transport
        with tempfile.TemporaryDirectory() as tmp:
            def fake_http(provider,model,text,deadline,audit=None,content_path=None):
                events=[]
                try:return real('{"summary.py":"synthetic source outside files wrapper"}',content_path,events,transport=original_transport)
                finally:
                    for event in events:
                        if audit:audit(event)
            with patch('model_adapter.http_complete',side_effect=fake_http),patch('executor.Executor.submit',side_effect=AssertionError('must not apply')),patch('signal.signal'),patch('signal.alarm'),patch.dict(os.environ,{'DEEPSEEK_API_KEY':'SYNTHETIC_LOCAL_SECRET_VALUE'}):
                with self.assertRaisesRegex(ProviderFault,'MODEL_SCHEMA_INVALID'):update_existing(tmp,profile['image_id'],base,acceptance)
            p=Path(tmp);audit=json.loads((p/'update-api-audit.json').read_text())
            self.assertEqual(audit['http_status'],'RESPONSE_RECEIVED');self.assertEqual(audit['schema_status'],'REJECTED');self.assertEqual(audit['update_status'],'NOT_APPLIED')
            self.assertEqual(audit['schema_error']['violation'],'root_keys');self.assertNotEqual(audit['status'],'OUTCOME_UNKNOWN')
            self.assertTrue((p/'raw-model-content.txt').exists())
            db=sqlite3.connect(p/'state/state.sqlite3');self.assertEqual(db.execute('SELECT task_id,phase FROM operations ORDER BY rowid').fetchall(),[('create-1','FAILED'),('create-1-repair-1','COMPLETE')]);self.assertEqual(db.execute('SELECT sha256 FROM projects').fetchone()[0],'634c9205351c84736e6af8b1d0c7301c239380d8014f39349f7d6d36db5aa17d');db.close()
    def test_http_error_is_confirmed_response(self):
        import urllib.error,tempfile,os
        from pathlib import Path
        from unittest.mock import patch
        from model_adapter import http_complete,ProviderFault
        from processes import Deadline
        class Fake:
            def open(self,*a,**kw):raise urllib.error.HTTPError('synthetic',429,'limited',{},None)
        events=[]
        with tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{'DEEPSEEK_API_KEY':'SYNTHETIC_LOCAL_SECRET_VALUE'}),patch('urllib.request.build_opener',return_value=Fake()):
            with self.assertRaisesRegex(ProviderFault,'HTTP_429'):http_complete('deepseek','deepseek-flash','synthetic',Deadline(30),events.append,Path(tmp)/'raw')
        self.assertEqual(events[-1]['http_status'],'RESPONSE_RECEIVED');self.assertEqual(events[-1]['http_code'],429);self.assertEqual(events[-1]['schema_status'],'NOT_RUN')
    def test_no_response_remains_unknown(self):
        import urllib.error,os
        from unittest.mock import patch
        from model_adapter import http_complete,ProviderFault
        from processes import Deadline
        class Fake:
            def open(self,*a,**kw):raise urllib.error.URLError('synthetic timeout')
        events=[]
        with patch.dict(os.environ,{'DEEPSEEK_API_KEY':'SYNTHETIC_LOCAL_SECRET_VALUE'}),patch('urllib.request.build_opener',return_value=Fake()):
            with self.assertRaisesRegex(ProviderFault,'OUTCOME_UNKNOWN'):http_complete('deepseek','deepseek-flash','synthetic',Deadline(30),events.append)
        self.assertEqual(events[-1]['http_status'],'OUTCOME_UNKNOWN');self.assertEqual(events[-1]['schema_status'],'NOT_RUN')
    def test_raw_capture_survives_malformed_usage_metadata(self):
        import tempfile,json
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw';events=[]
            content=json.dumps({'files':{'summary.py':'# synthetic'}})
            result=self.invoke(content,p,events,usage=['wrong type'])
            self.assertEqual(result['usage'],{});self.assertEqual(p.read_text(),content)
    def test_labelled_secret_and_existing_file_permissions(self):
        import tempfile,stat
        from pathlib import Path
        from model_adapter import safe_content_snapshot
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'raw';p.write_text('old');p.chmod(0o644)
            safe_content_snapshot(p,'{"password":"SYNTHETIC_PASSWORD_VALUE","api_key":"SYNTHETIC_KEY_VALUE"}')
            self.assertNotIn('SYNTHETIC_PASSWORD_VALUE',p.read_text());self.assertNotIn('SYNTHETIC_KEY_VALUE',p.read_text())
            self.assertEqual(stat.S_IMODE(p.stat().st_mode),0o600)
    def test_body_read_failure_does_not_erase_http_response(self):
        import os
        from unittest.mock import patch
        from model_adapter import http_complete,ProviderFault
        from processes import Deadline
        class Response:
            status=200
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def read(self,*a):raise TimeoutError('synthetic body timeout')
        class Fake:
            def open(self,*a,**kw):return Response()
        events=[]
        with patch.dict(os.environ,{'DEEPSEEK_API_KEY':'SYNTHETIC_LOCAL_SECRET_VALUE'}),patch('urllib.request.build_opener',return_value=Fake()):
            with self.assertRaisesRegex(ProviderFault,'RESPONSE_READ_FAILED'):http_complete('deepseek','deepseek-flash','synthetic',Deadline(30),events.append)
        self.assertEqual(events[-1]['http_status'],'RESPONSE_RECEIVED');self.assertEqual(events[-1]['schema_status'],'NOT_RUN')

# Offline diagnostic experiment ONLY. This is not adapter normalization.
def diagnostic_remove_type(raw):
    def unique(pairs):
        out={}
        for k,v in pairs:
            if k in out:raise ValueError('duplicate_key')
            out[k]=v
        return out
    obj=json.loads(raw,object_pairs_hook=unique)
    if not isinstance(obj,dict) or set(obj)!={'type','files'} or obj['type']!='json_object':
        raise ValueError('experiment_requires_exact_type_and_files')
    adjusted=json.dumps({'files':obj['files']},ensure_ascii=False)
    candidate(adjusted) # unchanged strict production schema
    return adjusted

class OfflineTypeRegressionTests(unittest.TestCase):
    def raw(self,**extra):return json.dumps({'files':{'summary.py':'print(1)','README.md':'usage'},**extra})
    def test_valid_strict(self):self.assertEqual(set(candidate(self.raw())),{'summary.py','README.md'})
    def test_original_stays_rejected(self):
        with self.assertRaises(ProviderFault):candidate(self.raw(type='json_object'))
    def test_only_exact_service_key_removed(self):
        raw=self.raw(type='json_object');before=json.loads(raw)
        self.assertEqual(json.loads(diagnostic_remove_type(raw)),{'files':before['files']})
        self.assertEqual(json.loads(raw),before)
    def test_arbitrary_extra_key_rejected(self):
        with self.assertRaises(ValueError):diagnostic_remove_type(self.raw(type='json_object',other=1))
    def test_wrong_type_rejected(self):
        for val in ['text',None,1,{'type':'json_object'}]:
            with self.assertRaises(ValueError):diagnostic_remove_type(self.raw(type=val))
    def test_duplicate_keys_rejected(self):
        with self.assertRaises(ValueError):diagnostic_remove_type('{"type":"text","type":"json_object","files":{}}')
    def test_bad_files_rejected(self):
        for files in [[],{}, {'summary.py':1},{'summary.py':'code','../escape':'bad'}]:
            with self.assertRaises(ProviderFault):diagnostic_remove_type(json.dumps({'type':'json_object','files':files}))
    def test_invalid_json_rejected(self):
        with self.assertRaises(ValueError):diagnostic_remove_type('{invalid')


def run_offline_type_experiment(received,output,image):
    import hashlib,sqlite3,socket,zipfile
    from isolation import PodmanBackend
    from contracts import verify
    received=Path(received);output=Path(output);output.mkdir(exist_ok=True)
    raw=(received/'raw-model-content.txt').read_bytes()
    assert hashlib.sha256(raw).hexdigest()=='4df3329b3818f9a268c83923dd929277349d692e6ae49fbb9cbe0ffa004dcb44'
    original=json.loads(raw);adjusted=diagnostic_remove_type(raw.decode());files=candidate(adjusted)
    assert original['files']==files
    # Baseline state is read-only, never sent to submit().
    state=received/'state/state.sqlite3';state_before=hashlib.sha256(state.read_bytes()).hexdigest()
    db=sqlite3.connect('file:'+str(state.resolve())+'?mode=ro',uri=True)
    history=db.execute('select task_id,phase from operations order by rowid').fetchall();db.close()
    assert history==[('create-1','FAILED'),('create-1-repair-1','COMPLETE')]
    checks={}
    with patch('socket.create_connection',side_effect=AssertionError('NO_API_NETWORK')),patch('socket.socket.connect',side_effect=AssertionError('NO_API_NETWORK')),patch('model_adapter.http_complete',side_effect=AssertionError('NO_MODEL_REQUEST')):
        with tempfile.TemporaryDirectory() as tmp:
            from executor import sha
            backend=PodmanBackend(image,sha(tmp.encode()));executor=Executor(Path(tmp)/'diagnostic-state',backend)
            deadline=Deadline(120);backend.begin('0'*64,deadline);isolation=backend.preflight(deadline)
            baseline=Path(tmp)/'baseline';executor._write_files(baseline,executor._archive(received/'baseline-product.zip','634c9205351c84736e6af8b1d0c7301c239380d8014f39349f7d6d36db5aa17d'))
            candidate_dir=Path(tmp)/'candidate';executor._write_files(candidate_dir,files)
            checks['baseline']=verify(baseline,'csv-v1',backend,deadline)
            checks['experimental_candidate']=verify(candidate_dir,'csv-v2',backend,deadline)
            assert (candidate_dir/'summary.py').read_bytes()==original['files']['summary.py'].encode()
            code_changed=(baseline/'summary.py').read_bytes()!=(candidate_dir/'summary.py').read_bytes()
    assert hashlib.sha256(state.read_bytes()).hexdigest()==state_before
    (output/'original-raw.txt').write_bytes(raw);(output/'experimental-response.json').write_text(adjusted)
    # Diagnostic export, never published as updated product.
    with zipfile.ZipFile(output/'experimental-candidate.zip','w',zipfile.ZIP_DEFLATED) as z:
        for name,source in sorted(files.items()):z.writestr(name,source.encode())
    result={'status':'OFFLINE_COUNTERFACTUAL_BEHAVIOR_PASS' if all(x['ok'] for x in checks.values()) else 'OFFLINE_COUNTERFACTUAL_BEHAVIOR_FAIL','original_update_verdict':'NO-GO','original_schema_rejected':True,'strict_schema_after_single_key_removal':True,'checks':checks,'isolation':isolation,'summary_code_changed':code_changed,'history':history,'existing_state_unchanged':True,'raw_sha256':hashlib.sha256(raw).hexdigest(),'experimental_response_sha256':hashlib.sha256(adjusted.encode()).hexdigest(),'experimental_zip_sha256':hashlib.sha256((output/'experimental-candidate.zip').read_bytes()).hexdigest(),'paid_model_requests':0,'executor_submit_calls':0,'new_create':False,'update_applied':False,'full_lifecycle_pass':False}
    (output/'experiment.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
    return result
