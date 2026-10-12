"""Task-scoped independent executor. SQLite commit + project lock; no Forge imports."""
import base64
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import time
import zipfile
from contracts import verify, VERSIONS
from processes import Deadline, Fault

MAX_BYTES=4*1024*1024


def canonical(value):return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()
def sha(data):return hashlib.sha256(data).hexdigest()


def safe(name):
    p=PurePosixPath(name)
    if (not name or p.is_absolute() or not p.parts or '..' in p.parts or '\\' in name
        or ':' in name or str(p)!=name or any(part.startswith('.') for part in p.parts)):
        raise Fault('PATH_OUTSIDE_PROJECT')
    return name


def normalize(task):
    required={'project_id','task_id','mode','request','files','oracle_version','deadline_seconds'}
    if not isinstance(task,dict) or not required.issubset(task) or set(task)-required-{'base_sha256'}:
        raise Fault('TASK_SCHEMA_INVALID')
    for key in ['project_id','task_id']:
        if not isinstance(task[key],str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}',task[key]):raise Fault('IDENTITY_INVALID')
    if task['mode'] not in ('CREATE','UPDATE') or task['oracle_version'] not in VERSIONS:raise Fault('TASK_OPERATION_INVALID')
    if not isinstance(task['request'],str) or not task['request'].strip() or len(task['request'])>4096:raise Fault('REQUEST_INVALID')
    seconds=task['deadline_seconds']
    if isinstance(seconds,bool) or not isinstance(seconds,(int,float)) or not 0.1<=seconds<=120:raise Fault('DEADLINE_INVALID')
    files=task['files']
    if not isinstance(files,dict) or not 1<=len(files)<=50:raise Fault('FILES_INVALID')
    total=0
    for name,content in files.items():
        if not isinstance(name,str) or not isinstance(content,str):raise Fault('FILES_INVALID')
        safe(name);total+=len(content.encode())
    if total>MAX_BYTES:raise Fault('INPUT_LIMIT')
    if task['mode']=='UPDATE' and not re.fullmatch(r'[a-f0-9]{64}',task.get('base_sha256','')):raise Fault('BASE_HASH_REQUIRED')
    if task['mode']=='CREATE' and task.get('base_sha256'):raise Fault('CREATE_BASE_FORBIDDEN')
    return json.loads(canonical(task))


class Executor:
    def __init__(self, root, backend):
        if Path(root).is_symlink():raise Fault('STATE_SYMLINK')
        self.root=Path(root).resolve();self.root.mkdir(parents=True,exist_ok=True);self.root.chmod(0o700)
        self.backend=backend
        for name in ['locks','staging','products']:(self.root/name).mkdir(exist_ok=True)
        with self.db() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS projects(project_id TEXT PRIMARY KEY,sha256 TEXT,operation_id TEXT,oracle_version TEXT);
            CREATE TABLE IF NOT EXISTS operations(operation_id TEXT PRIMARY KEY,project_id TEXT,task_id TEXT,
              payload_hash TEXT,task TEXT,phase TEXT,expires_at REAL,result TEXT,
              UNIQUE(project_id,task_id));
            ''')

    @contextmanager
    def db(self):
        db=sqlite3.connect(self.root/'state.sqlite3',timeout=0.05)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:yield db
        finally:db.close()

    @contextmanager
    def lock(self, project_id, deadline):
        with (self.root/'locks'/(project_id+'.lock')).open('a') as lock:
            while True:
                deadline.remaining()
                try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);break
                except BlockingIOError:time.sleep(min(0.01,deadline.remaining()))
            try:yield
            finally:fcntl.flock(lock,fcntl.LOCK_UN)

    def head(self, project_id):
        with self.db() as db:
            row=db.execute('SELECT * FROM projects WHERE project_id=?',(project_id,)).fetchone()
            return dict(row) if row else None

    def _archive(self, path, expected=None):
        if not path.is_file() or (expected and sha(path.read_bytes())!=expected):raise Fault('ARTIFACT_CHANGED')
        files={}
        try:
            with zipfile.ZipFile(path) as z:
                if len(z.infolist())>50 or len(set(z.namelist()))!=len(z.namelist()):raise Fault('ARCHIVE_INVALID')
                if sum(x.file_size for x in z.infolist())>MAX_BYTES:raise Fault('ARCHIVE_LIMIT')
                for entry in z.infolist():
                    safe(entry.filename)
                    if entry.is_dir() or ((entry.external_attr>>16)&0o170000)==0o120000:raise Fault('ARCHIVE_INVALID')
                    files[entry.filename]=z.read(entry.filename)
        except (OSError,zipfile.BadZipFile) as exc:raise Fault('ARCHIVE_INVALID') from exc
        return files

    def _write_files(self, workspace, files):
        workspace.mkdir(parents=True,exist_ok=True);workspace.chmod(0o755)
        for name,data in files.items():
            path=workspace/name;path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(data);path.chmod(0o644)
        for path in workspace.rglob('*'):
            if path.is_dir():path.chmod(0o755)

    def _package(self, path, files, deadline):
        part=path.with_suffix('.part')
        try:
            with zipfile.ZipFile(part,'w',zipfile.ZIP_DEFLATED) as z:
                for name,data in sorted(files.items()):
                    deadline.remaining()
                    item=zipfile.ZipInfo(name,(2026,1,1,0,0,0));item.compress_type=zipfile.ZIP_DEFLATED
                    item.external_attr=0o100644<<16;z.writestr(item,data)
            with part.open('rb') as stream:os.fsync(stream.fileno())
            os.replace(part,path)
            fd=os.open(path.parent,os.O_DIRECTORY)
            try:os.fsync(fd)
            finally:os.close(fd)
        finally:part.unlink(missing_ok=True)

    def cleanup(self, project_id=None):
        """Reap only journal-owned inactive operation directories; skip live locks."""
        removed=[]
        with self.db() as db:
            rows=db.execute('SELECT operation_id,project_id,phase,result FROM operations'+
                            (' WHERE project_id=?' if project_id else ''),
                            (project_id,) if project_id else ()).fetchall()
        for row in rows:
            path=self.root/'staging'/row['operation_id']
            part=self.root/'products'/(row['operation_id']+'.part')
            if not path.exists() and not part.exists() and row['phase'] in ('COMPLETE','FAILED'):
                if 'CONTAINER_CLEANUP' not in (row['result'] or ''):continue
            try:
                with self.lock(row['project_id'],Deadline(0.02)):
                    if hasattr(self.backend,'cleanup_operation'):
                        self.backend.cleanup_operation(row['operation_id'],Deadline(3))
                    if path.exists():shutil.rmtree(path);removed.append(row['operation_id'])
                    part.unlink(missing_ok=True)
            except Fault:continue
        return removed

    def fetch(self, project_id, operation_id):
        if not re.fullmatch(r'[a-f0-9]{64}',operation_id):raise Fault('OPERATION_ID_INVALID')
        with self.db() as db:
            row=db.execute('SELECT * FROM operations WHERE operation_id=? AND project_id=?',(operation_id,project_id)).fetchone()
        if not row or row['phase']!='COMPLETE':raise Fault('ARTIFACT_NOT_COMMITTED')
        result=json.loads(row['result']);path=self.root/'products'/(operation_id+'.zip')
        self._archive(path,result['sha256'])
        return {**result,'reused':True,'artifact_base64':base64.b64encode(path.read_bytes()).decode()}

    def inspect(self, project_id, operation_id):
        if not re.fullmatch(r'[a-f0-9]{64}',operation_id):raise Fault('OPERATION_ID_INVALID')
        with self.db() as db:
            row=db.execute('SELECT phase,result,expires_at FROM operations WHERE project_id=? AND operation_id=?',
                           (project_id,operation_id)).fetchone()
        if not row:return {'status':'NOT_FOUND','operation_id':operation_id,'project_id':project_id}
        return {'status':'OBSERVED','project_id':project_id,'operation_id':operation_id,
                'phase':row['phase'],'expired':time.time()>row['expires_at'],
                'result':json.loads(row['result']) if row['result'] else None}

    def _promote(self, task, op, result, deadline):
        deadline.remaining()
        with self.db() as db:
            row=db.execute('SELECT * FROM projects WHERE project_id=?',(task['project_id'],)).fetchone()
            if task['mode']=='CREATE' and row:raise Fault('PROJECT_EXISTS')
            if task['mode']=='UPDATE' and (not row or row['sha256']!=task['base_sha256']):raise Fault('BASELINE_CHANGED')
            if row and row['oracle_version']=='csv-v2' and task['oracle_version']=='csv-v1':raise Fault('CONTRACT_DOWNGRADE')
            db.execute('INSERT OR REPLACE INTO projects VALUES(?,?,?,?)',
                       (task['project_id'],result['sha256'],op,task['oracle_version']))
            db.execute('UPDATE operations SET phase=?,result=? WHERE operation_id=?',('COMPLETE',canonical(result).decode(),op))

    def submit(self, task, *, fault_hook=None):
        task=normalize(task);deadline=Deadline(task['deadline_seconds'])
        project=task['project_id'];payload_hash=sha(canonical(task));op=sha(canonical([project,task['task_id'],payload_hash]))
        workspace=self.root/'staging'/op;artifact=self.root/'products'/(op+'.zip')
        with self.lock(project,deadline):
            with self.db() as db:
                prior=db.execute('SELECT * FROM operations WHERE project_id=? AND task_id=?',(project,task['task_id'])).fetchone()
            if prior and prior['payload_hash']!=payload_hash:raise Fault('TASK_IDENTITY_COLLISION')
            if prior and prior['phase']=='COMPLETE':return self.fetch(project,op)
            if prior and prior['phase']=='FAILED':return json.loads(prior['result'])
            if prior:deadline.expires_at=prior['expires_at']
            else:
                expires=time.time()+task['deadline_seconds'];deadline.expires_at=expires
                with self.db() as db:db.execute('INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)',
                    (op,project,task['task_id'],payload_hash,canonical(task).decode(),'INTENT',expires,None))
            began=time.monotonic()
            try:
                if workspace.exists():shutil.rmtree(workspace)
                deadline.remaining()
                if prior and prior['phase']=='VERIFIED':
                    result=json.loads(prior['result']);self._archive(artifact,result['sha256'])
                    self._promote(task,op,result,deadline)
                    return self.fetch(project,op)
                current=self.head(project)
                if task['mode']=='CREATE' and current:raise Fault('PROJECT_EXISTS')
                if task['mode']=='UPDATE' and (not current or current['sha256']!=task['base_sha256']):raise Fault('BASELINE_CHANGED')
                if current and current['oracle_version']=='csv-v2' and task['oracle_version']=='csv-v1':raise Fault('CONTRACT_DOWNGRADE')
                files=self._archive(self.root/'products'/(current['operation_id']+'.zip'),current['sha256']) if current else {}
                files.update({name:text.encode() for name,text in task['files'].items()})
                if sum(map(len,files.values()))>MAX_BYTES:raise Fault('INPUT_LIMIT')
                self._write_files(workspace/'candidate',files)
                if fault_hook:fault_hook('staged')
                if hasattr(self.backend,'begin'):self.backend.begin(op,deadline)
                isolation=self.backend.preflight(deadline)
                first=verify(workspace/'candidate',task['oracle_version'],self.backend,deadline)
                if not first['ok']:raise Fault('BEHAVIOR_FAILED')
                self._package(artifact,files,deadline)
                delivered=self._archive(artifact)
                self._write_files(workspace/'delivered',delivered)
                second=verify(workspace/'delivered',task['oracle_version'],self.backend,deadline)
                if not second['ok']:raise Fault('DELIVERY_BEHAVIOR_FAILED')
                result={'status':'VERIFIED_ARTIFACT' if self.backend.assurance=='ROOTLESS_PODMAN' else 'VERIFIED_TEST_FIXTURE',
                        'project_id':project,'task_id':task['task_id'],'operation_id':op,
                        'sha256':sha(artifact.read_bytes()),'files':{k:sha(v) for k,v in files.items()},
                        'oracle_version':task['oracle_version'],'isolation':isolation,
                        'checks':first,'delivery_checks':second,'elapsed_seconds':time.monotonic()-began,
                        'api_calls':0,'reused':False}
                with self.db() as db:db.execute('UPDATE operations SET phase=?,result=? WHERE operation_id=?',
                                               ('VERIFIED',canonical(result).decode(),op))
                if fault_hook:fault_hook('verified_before_commit')
                self._promote(task,op,result,deadline)
                return {**result,'artifact_base64':base64.b64encode(artifact.read_bytes()).decode()}
            except (Fault,OSError,sqlite3.Error,ValueError) as exc:
                result={'status':'FAILED','reason':str(exc),'project_id':project,'task_id':task['task_id'],'operation_id':op}
                with self.db() as db:db.execute('UPDATE operations SET phase=?,result=? WHERE operation_id=?',('FAILED',canonical(result).decode(),op))
                if not self.head(project) or self.head(project)['operation_id']!=op:artifact.unlink(missing_ok=True)
                return result
            finally:
                if workspace.exists():shutil.rmtree(workspace)
