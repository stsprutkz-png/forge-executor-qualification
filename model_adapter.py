"""Controller-side model adapter. No Core V2, no candidate execution, no mock fallback."""
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
import urllib.request
import urllib.error
from executor import normalize, canonical, sha
from processes import Deadline, Fault

ENDPOINTS={'deepseek':('https://api.deepseek.com/chat/completions','DEEPSEEK_API_KEY'),
           'openrouter':('https://openrouter.ai/api/v1/chat/completions','OPENROUTER_API_KEY')}
SECRET=re.compile(r'-----BEGIN .*PRIVATE KEY|(?:sk-|ghp_|github_pat_)[A-Za-z0-9_-]{16,}')

class ProviderFault(Fault):
    pass

def candidate(raw):
    if not isinstance(raw,str) or len(raw.encode())>65536:raise ProviderFault('MODEL_OUTPUT_LIMIT')
    if SECRET.search(raw):raise ProviderFault('MODEL_SECRET_REJECTED')
    def pairs(items):
        d={}
        for k,v in items:
            if k in d:raise ProviderFault('MODEL_DUPLICATE_KEY')
            d[k]=v
        return d
    try:v=json.loads(raw,object_pairs_hook=pairs)
    except (ValueError,TypeError):raise ProviderFault('MODEL_JSON_INVALID') from None
    if not isinstance(v,dict) or set(v)!={'files'} or not isinstance(v['files'],dict):raise ProviderFault('MODEL_SCHEMA_INVALID')
    if not v['files'] or set(v['files'])-{'summary.py','README.md'} or 'summary.py' not in v['files']:
        raise ProviderFault('MODEL_FILES_OUTSIDE_CONTRACT')
    if any(not isinstance(x,str) for x in v['files'].values()):raise ProviderFault('MODEL_FILES_INVALID')
    return v['files']

def prompt(spec):
    """Only a fixed synthetic contract and optional verified synthetic baseline go out."""
    version=spec['oracle_version']
    base=spec.get('baseline_files',{})
    if not isinstance(base,dict) or set(base)-{'summary.py','README.md'}:raise Fault('BASELINE_INVALID')
    if any(not isinstance(x,str) or SECRET.search(x) for x in base.values()):raise Fault('BASELINE_SECRET_OR_INVALID')
    contract=('Implement summary.py in Python stdlib. Usage: python summary.py input.csv. '
              'Read UTF-8 BOM CSV with amount column. Output one JSON object with rows integer '
              'and total exact Decimal string, preserving decimal scale (0.1+0.2=0.3, '
              '10-3+2.50=9.50). Support numbers exceeding 28 digits without rounding. '
              'Empty data produces rows=0,total="0". Invalid amount, NaN/Infinity, missing amount '
              'column or I/O error: exit code 2, no stdout, diagnostic only on stderr. '
              'No networking, external dependencies, subprocesses, credentials or PASS markers. ')
    if version=='csv-v2':contract+='Also include categories: sorted mapping of category (empty if absent) to exact Decimal total strings. '
    if spec['mode']=='UPDATE':contract+='Update the supplied verified baseline, preserving previous behavior. '
    return contract+'Return one JSON object with only the key files. The files mapping must contain summary.py with the entire executable Python source and README.md with actual usage instructions. Generate the implementation: do not return labels such as complete source or usage as file contents. No fences or extra keys.\nVERIFIED SYNTHETIC BASELINE:\n'+json.dumps(base,sort_keys=True)

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args):return None

def http_complete(provider,model,text,deadline,audit=None):
    if provider not in ENDPOINTS or not isinstance(model,str) or not model.strip():raise ProviderFault('PROVIDER_CONFIG_INVALID')
    url,env=ENDPOINTS[provider];key=os.environ.get(env)
    if not key:raise ProviderFault('PROVIDER_CREDENTIAL_MISSING')
    if key in text:raise ProviderFault('PROMPT_SECRET_REJECTED')
    body={'model':model,'messages':[{'role':'user','content':text}],'max_tokens':2048,
          'stream':False,'response_format':{'type':'json_object'}}
    if len(text.encode())>8192:raise ProviderFault('PROVIDER_INPUT_LIMIT')
    if provider=='deepseek':body['thinking']={'type':'disabled'}
    if provider=='openrouter':body['provider']={'require_parameters':True,'allow_fallbacks':False}
    request=urllib.request.Request(url,data=canonical(body),headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},method='POST')
    started=time.monotonic()
    try:
        with urllib.request.build_opener(NoRedirect()).open(request,timeout=min(30,deadline.remaining())) as r:
            raw=r.read(262145)
    except urllib.error.HTTPError as e:raise ProviderFault('PROVIDER_HTTP_'+str(e.code)) from None
    except (urllib.error.URLError,TimeoutError,OSError):raise ProviderFault('PROVIDER_OUTCOME_UNKNOWN') from None
    deadline.remaining()
    if len(raw)>262144:raise ProviderFault('PROVIDER_RESPONSE_LIMIT')
    try:
        v=json.loads(raw);choice=v['choices'][0]
        content=choice['message']['content'];usage=v.get('usage',{})
        if key in content:raise ProviderFault('MODEL_SECRET_REJECTED')
    except (KeyError,TypeError,ValueError,IndexError):raise ProviderFault('PROVIDER_RESPONSE_INVALID') from None
    # Whitelist metadata; never persist headers, arbitrary error bodies or reasoning.
    response_id=str(v.get('id',''))[:200];returned_model=str(v.get('model',model))[:200]
    if any(key in x or SECRET.search(x) for x in [response_id,returned_model]):raise ProviderFault('MODEL_SECRET_REJECTED')
    safe_usage={k:x for k,x in usage.items() if k in ('prompt_tokens','completion_tokens','total_tokens','cost','prompt_cache_hit_tokens','prompt_cache_miss_tokens') and isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x) and x>=0}
    safe={'response_id':response_id,'provider':provider,'model':returned_model,'usage':safe_usage,'elapsed_seconds':time.monotonic()-started,'provenance':'LIVE_PROVIDER_API','cost_usd_observed':safe_usage.get('cost') if provider=='openrouter' else None}
    if audit:audit(safe)
    if choice['finish_reason']!='stop' or choice['message'].get('tool_calls'):raise ProviderFault('PROVIDER_COMPLETION_INCOMPLETE')
    candidate(content)
    return {'content':content,'response_id':response_id,'provider':provider,'model':returned_model,
            'usage':safe_usage,'elapsed_seconds':time.monotonic()-started,'provenance':'LIVE_PROVIDER_API',
            'cost_usd_observed':safe_usage.get('cost') if provider=='openrouter' else None}

class Adapter:
    """Durable identity + frozen generated task. Ambiguous generation never auto-replays."""
    def __init__(self,root,transport=http_complete):
        root=Path(root)
        if root.is_symlink():raise Fault('ADAPTER_STATE_SYMLINK')
        self.root=root.resolve();self.root.mkdir(parents=True,exist_ok=True);self.root.chmod(0o700)
        self.transport=transport
        db=self.db()
        try:
            db.execute('CREATE TABLE IF NOT EXISTS jobs(project TEXT,taskid TEXT,spec_hash TEXT,phase TEXT,result TEXT,PRIMARY KEY(project,taskid))');db.commit()
        finally:db.close()
    def db(self):
        db=sqlite3.connect(self.root/'adapter.sqlite3',timeout=1);db.execute('PRAGMA synchronous=FULL');return db
    def prepare(self,spec,providers):
        allowed={'project_id','task_id','mode','oracle_version','deadline_seconds','base_sha256','baseline_files'}
        if not isinstance(spec,dict) or set(spec)-allowed:raise Fault('CONTROLLER_SPEC_INVALID')
        seed={k:v for k,v in spec.items() if k!='baseline_files'}
        seed['request']='Synthetic CSV model integration';seed['files']={'summary.py':'# placeholder'}
        normalize(seed) # identities, deadline, baseline and contract remain controller-owned.
        text=prompt(spec);spec_hash=sha(canonical(spec));deadline=Deadline(60)
        with (self.root/'generation.lock').open('a') as lock:
            while True:
                deadline.remaining()
                try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);break
                except BlockingIOError:time.sleep(min(.01,deadline.remaining()))
            db=self.db()
            try:
                row=db.execute('SELECT spec_hash,phase,result FROM jobs WHERE project=? AND taskid=?',(spec['project_id'],spec['task_id'])).fetchone()
                if row:
                    if row[0]!=spec_hash:raise Fault('ADAPTER_IDENTITY_COLLISION')
                    if row[1]!='PREPARED':raise Fault('GENERATION_OUTCOME_UNRESOLVED')
                    result=json.loads(row[2]);assert sha(canonical(result['task']))==result['task_sha256']
                    return {**result,'reused':True}
                db.execute('INSERT INTO jobs VALUES(?,?,?,?,?)',(spec['project_id'],spec['task_id'],spec_hash,'GENERATING',None));db.commit()
                attempts=[]
                for provider,model in providers:
                    deadline.remaining()
                    try:
                        response=self.transport(provider,model,text,deadline)
                        files=candidate(response['content']);task=normalize({**seed,'files':files})
                    except ProviderFault as e:
                        attempts.append({'provider':provider,'model':model,'status':str(e)})
                        # An ambiguous request may have incurred cost; don't replay/failover silently.
                        if str(e)=='PROVIDER_OUTCOME_UNKNOWN':break
                        continue
                    result={'task':task,'task_sha256':sha(canonical(task)),'raw_model_content':response['content'],
                            'raw_model_sha256':sha(response['content'].encode()),'prompt_sha256':sha(text.encode()),
                            'receipt':{k:v for k,v in response.items() if k!='content'},'attempts':attempts,
                            'reused':False,'executor_verified':False}
                    db.execute('UPDATE jobs SET phase=?,result=? WHERE project=? AND taskid=?',('PREPARED',canonical(result).decode(),spec['project_id'],spec['task_id']));db.commit()
                    return result
                result={'status':'BLOCKED','attempts':attempts,'real_model_artifact':False}
                db.execute('UPDATE jobs SET phase=?,result=? WHERE project=? AND taskid=?',('BLOCKED',canonical(result).decode(),spec['project_id'],spec['task_id']));db.commit()
                return result
            finally:db.close();fcntl.flock(lock,fcntl.LOCK_UN)
