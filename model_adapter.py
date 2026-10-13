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
    def __init__(self,code,detail=None):
        super().__init__(code);self.detail=detail or {}

def redact(value):
    text=value if isinstance(value,str) else json.dumps(value,ensure_ascii=False,default=lambda unused:'[UNSERIALIZABLE]')
    keys=[os.environ.get(env) for _,env in ENDPOINTS.values()]
    for key in sorted((k for k in keys if k),key=len,reverse=True):text=text.replace(key,'[REDACTED]')
    text=re.sub(r'-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)','[REDACTED PRIVATE KEY]',text)
    text=re.sub(r'(?:sk-|gh[pousr]_|github_pat_)[A-Za-z0-9_-]{16,}','[REDACTED TOKEN]',text)
    text=re.sub(r'(?i)(bearer\s+)[^\s"\x27]+',r'\1[REDACTED]',text)
    text=re.sub(r'''(?i)(["']?(?:password|api[_-]?key|access[_-]?token|secret|authorization)["']?\s*[:=]\s*["'])[^"']*(["'])''',r'\1[REDACTED]\2',text)
    return text

def safe_content_snapshot(path,content):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    text=redact(content);data=text.encode();limit=65536
    bounded=data[:limit].decode('utf-8',errors='ignore').encode()
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC|os.O_NOFOLLOW,0o600)
    os.fchmod(fd,0o600)
    with os.fdopen(fd,'wb') as f:f.write(bounded);f.flush();os.fsync(f.fileno())
    return {'path':path.name,'content_type':type(content).__name__,'original_bytes':len(content.encode()) if isinstance(content,str) else None,'saved_bytes':len(bounded),'truncated':len(data)>limit,'redacted':text!=content,'saved_sha256':sha(bounded)}

def candidate(raw):
    if not isinstance(raw,str):raise ProviderFault('MODEL_SCHEMA_INVALID',{'violation':'content_type','actual_type':type(raw).__name__})
    if len(raw.encode())>65536:raise ProviderFault('MODEL_OUTPUT_LIMIT',{'violation':'content_size','max_bytes':65536})
    if SECRET.search(raw) or any(os.environ.get(env) and os.environ[env] in raw for _,env in ENDPOINTS.values()):raise ProviderFault('MODEL_SECRET_REJECTED',{'violation':'secret_detected'})
    def pairs(items):
        d={}
        for k,v in items:
            if k in d:raise ProviderFault('MODEL_DUPLICATE_KEY',{'violation':'duplicate_key','key':redact(k)[:200]})
            d[k]=v
        return d
    def invalid_constant(unused):raise ProviderFault('MODEL_JSON_INVALID',{'violation':'nonfinite_json_constant'})
    try:v=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid_constant)
    except json.JSONDecodeError as e:raise ProviderFault('MODEL_JSON_INVALID',{'violation':'json_syntax','line':e.lineno,'column':e.colno,'offset':e.pos}) from None
    if not isinstance(v,dict):raise ProviderFault('MODEL_SCHEMA_INVALID',{'violation':'root_type','actual_type':type(v).__name__})
    if set(v)!={'files'}:raise ProviderFault('MODEL_SCHEMA_INVALID',{'violation':'root_keys','actual_keys':[redact(k)[:200] for k in sorted(v)][:50],'required_keys':['files']})
    if not isinstance(v['files'],dict):raise ProviderFault('MODEL_SCHEMA_INVALID',{'violation':'files_type','actual_type':type(v['files']).__name__})
    missing={'summary.py'}-set(v['files']);outside=set(v['files'])-{'summary.py','README.md'}
    if missing:raise ProviderFault('MODEL_FILES_OUTSIDE_CONTRACT',{'violation':'missing_required_files','files':sorted(missing)})
    if outside:raise ProviderFault('MODEL_FILES_OUTSIDE_CONTRACT',{'violation':'files_outside_contract','files':[redact(k)[:200] for k in sorted(outside)][:50]})
    bad=[{'file':k,'actual_type':type(x).__name__} for k,x in v['files'].items() if not isinstance(x,str)]
    if bad:raise ProviderFault('MODEL_FILES_INVALID',{'violation':'file_content_type','files':bad})
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

def http_complete(provider,model,text,deadline,audit=None,content_path=None):
    if provider not in ENDPOINTS or not isinstance(model,str) or not model.strip():raise ProviderFault('PROVIDER_CONFIG_INVALID')
    url,env=ENDPOINTS[provider];key=os.environ.get(env)
    if not key:raise ProviderFault('PROVIDER_CREDENTIAL_MISSING')
    if key in text:raise ProviderFault('PROMPT_SECRET_REJECTED')
    body={'model':model,'messages':[{'role':'user','content':text}],'max_tokens':2048,'stream':False,'response_format':{'type':'json_object'}}
    if len(text.encode())>8192:raise ProviderFault('PROVIDER_INPUT_LIMIT')
    if provider=='deepseek':body['thinking']={'type':'disabled'}
    if provider=='openrouter':body['provider']={'require_parameters':True,'allow_fallbacks':False}
    request=urllib.request.Request(url,data=canonical(body),headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},method='POST')
    started=time.monotonic();event={'http_status':'NOT_SENT','schema_status':'NOT_RUN'}
    def emit():
        if audit:audit(dict(event))
    try:
        with urllib.request.build_opener(NoRedirect()).open(request,timeout=min(30,deadline.remaining())) as r:
            event.update(http_status='RESPONSE_RECEIVED',http_code=getattr(r,'status',200));emit()
            raw=r.read(262145)
    except urllib.error.HTTPError as e:
        event.update(http_status='RESPONSE_RECEIVED',http_code=e.code);emit()
        raise ProviderFault('PROVIDER_HTTP_'+str(e.code)) from None
    except (urllib.error.URLError,TimeoutError,OSError):
        if event['http_status']!='RESPONSE_RECEIVED':event['http_status']='OUTCOME_UNKNOWN'
        emit()
        code='PROVIDER_RESPONSE_READ_FAILED' if event['http_status']=='RESPONSE_RECEIVED' else 'PROVIDER_OUTCOME_UNKNOWN'
        raise ProviderFault(code) from None
    if len(raw)>262144:raise ProviderFault('PROVIDER_RESPONSE_LIMIT',{'violation':'provider_envelope_size'})
    try:
        v=json.loads(raw);choice=v['choices'][0];content=choice['message']['content'];usage=v.get('usage',{})
    except (KeyError,TypeError,ValueError,IndexError):
        event.update(schema_status='ENVELOPE_REJECTED',schema_error={'violation':'provider_envelope_invalid'});emit()
        raise ProviderFault('PROVIDER_RESPONSE_INVALID') from None
    # Save sanitized raw model content BEFORE deadline/completion/schema rejection.
    if content_path:event['content_snapshot']=safe_content_snapshot(content_path,content)
    event.update(response_id=redact(str(v.get('id','')))[:200],provider=provider,model=redact(str(v.get('model',model)))[:200],
                 usage={k:x for k,x in (usage.items() if isinstance(usage,dict) else []) if k in ('prompt_tokens','completion_tokens','total_tokens','cost','prompt_cache_hit_tokens','prompt_cache_miss_tokens') and isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x) and x>=0},
                 elapsed_seconds=time.monotonic()-started,provenance='LIVE_PROVIDER_API')
    event['cost_usd_observed']=event['usage'].get('cost') if provider=='openrouter' else None;emit()
    try:
        deadline.remaining()
        if choice.get('finish_reason')!='stop' or choice['message'].get('tool_calls'):raise ProviderFault('PROVIDER_COMPLETION_INCOMPLETE',{'violation':'completion_not_final'})
        candidate(content)
    except ProviderFault as e:
        event.update(schema_status='REJECTED',schema_code=str(e),schema_error=e.detail);emit();raise
    event.update(schema_status='ACCEPTED');emit()
    return {'content':content,**event}

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


def normalize_recorded_update_envelope(raw):
    """Explicit owner-authorized recorded-response envelope, not generic tolerance."""
    if not isinstance(raw,str) or len(raw.encode('utf-8'))>65536:raise ProviderFault('RECORDED_ENVELOPE_REJECTED',{'violation':'content_limit_or_type'})
    def unique(pairs):
        out={}
        for k,v in pairs:
            if k in out:raise ValueError('duplicate_key')
            out[k]=v
        return out
    try:obj=json.loads(raw,object_pairs_hook=unique,parse_constant=lambda s:(_ for _ in ()).throw(ValueError('nonfinite_json')))
    except ValueError as e:raise ProviderFault('RECORDED_ENVELOPE_REJECTED',{'violation':'invalid_or_duplicate_json'}) from e
    if not isinstance(obj,dict) or set(obj)!={'files','type'} or obj['type']!='json_object':
        raise ProviderFault('RECORDED_ENVELOPE_REJECTED',{'violation':'exact_recorded_envelope_required'})
    adjusted=json.dumps({'files':obj['files']},ensure_ascii=False)
    files=candidate(adjusted)
    if set(files)!={'summary.py','README.md'}:raise ProviderFault('RECORDED_ENVELOPE_REJECTED',{'violation':'exact_update_files_required'})
    return adjusted,files
