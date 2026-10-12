"""Second host: independently verify downloaded ZIP, SHA and behavior under Podman."""
import argparse
import json
from pathlib import Path
import tempfile
from executor import Executor, sha
from isolation import PodmanBackend
from contracts import verify
from processes import Deadline

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--image',required=True);p.add_argument('--first',required=True);p.add_argument('--second',required=True);a=p.parse_args()
    first=Path(a.first);second=Path(a.second)
    data=(first/'product.zip').read_bytes();other=(second/'product.zip').read_bytes()
    metadata=json.loads((first/'remote-cycle.json').read_text())
    assert data==other and sha(data)==metadata['product_sha256']
    with tempfile.TemporaryDirectory(prefix='forge-delivery-controller-') as tmp:
        backend=PodmanBackend(a.image,sha(tmp.encode()));executor=Executor(Path(tmp)/'control',backend)
        candidate=Path(tmp)/'product';files=executor._archive(first/'product.zip',metadata['product_sha256'])
        executor._write_files(candidate,files)
        backend.begin('0'*64,Deadline(60));isolation=backend.preflight(Deadline(60))
        checks=verify(candidate,'csv-v2',backend,Deadline(60));assert checks['ok']
        result={'status':'CROSS_HOST_DELIVERY_CHECK','sha256':sha(data),'downloaded_twice_equal':True,
                'isolation':isolation,'checks':checks,'provider_verified_externally':False}
        print(json.dumps(result,indent=2));(first/'delivery-controller.json').write_text(json.dumps(result,indent=2))
