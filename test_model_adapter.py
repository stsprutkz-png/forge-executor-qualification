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
