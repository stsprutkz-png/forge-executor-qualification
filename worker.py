"""JSON stdin/stdout worker suitable for a forced SSH command. No public server."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parent))
from executor import Executor, sha, canonical, normalize, MAX_BYTES
from isolation import PodmanBackend
from processes import command, Deadline, Fault


def child(root, image, request):
    backend=PodmanBackend(image,sha(str(Path(root).resolve()).encode()))
    executor=Executor(root,backend)
    if request.get('action')=='SUBMIT':
        executor.cleanup(normalize(request['task'])['project_id'])
        return executor.submit(request['task'])
    if request.get('action')=='FETCH' and set(request)=={'action','project_id','operation_id'}:
        return executor.fetch(request['project_id'],request['operation_id'])
    if request.get('action')=='STATUS' and set(request)=={'action','project_id','operation_id'}:
        return executor.inspect(request['project_id'],request['operation_id'])
    raise Fault('REQUEST_ACTION_INVALID')


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--root',required=True);ap.add_argument('--image',required=True)
    ap.add_argument('--child-request');args=ap.parse_args()
    if args.child_request:
        request=json.loads(Path(args.child_request).read_text())
        try:result=child(args.root,args.image,request)
        except (Fault,OSError,ValueError) as exc:result={'status':'FAILED','reason':str(exc)}
        print(json.dumps(result,ensure_ascii=False,sort_keys=True));return
    raw=sys.stdin.buffer.read(MAX_BYTES+1)
    if len(raw)>MAX_BYTES:raise Fault('INPUT_LIMIT')
    request=json.loads(raw)
    if not isinstance(request,dict):raise Fault('REQUEST_INVALID')
    if request.get('action')=='SUBMIT':
        if set(request)!={'action','task'}:raise Fault('REQUEST_INVALID')
        task=normalize(request['task']);seconds=task['deadline_seconds']
        op=sha(canonical([task['project_id'],task['task_id'],sha(canonical(task))]))
    elif request.get('action') in ('FETCH','STATUS'):seconds=10;op=None
    else:raise Fault('REQUEST_ACTION_INVALID')
    with tempfile.TemporaryDirectory(prefix='forge-controller-request-') as tmp:
        path=Path(tmp)/'request.json';path.write_bytes(raw)
        try:
            run=command([sys.executable,'-I',str(Path(__file__).resolve()),'--root',args.root,
                         '--image',args.image,'--child-request',str(path)],Deadline(seconds),limit=2*MAX_BYTES)
            if run['exit_code']:raise Fault('WORKER_PROCESS_FAILED')
            result=json.loads(run['stdout'])
        except Fault as exc:
            result={'status':'OUTCOME_UNKNOWN' if op else 'FAILED','reason':str(exc)}
            if op:result['operation_id']=op
            if op:
                try:PodmanBackend(args.image,sha(str(Path(args.root).resolve()).encode())).cleanup_operation(op,Deadline(3))
                except (Fault,OSError):result['cleanup_status']='UNPROVEN'
        print(json.dumps(result,ensure_ascii=False,sort_keys=True))


if __name__=='__main__':
    try:main()
    except (Fault,OSError,ValueError,KeyError,TypeError) as exc:
        print(json.dumps({'status':'FAILED','reason':str(exc)},sort_keys=True))
