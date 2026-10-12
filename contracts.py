"""Controller-owned oracle; no product-authored tests or PASS markers."""
import json
from pathlib import Path
from processes import Deadline, Fault

VERSIONS={'csv-v1','csv-v2'}
CASES=[
 ('amount,category\n0.1,A\n0.2,A\n',{'total':'0.3','rows':2},{'A':'0.3'}),
 ('amount,category\n10,A\n-3,B\n2.50,A\n',{'total':'9.50','rows':3},{'A':'12.50','B':'-3'}),
 ('amount,category\n',{'total':'0','rows':0},{}),
 ('amount,category\nabc,A\n',None,None),('amount,category\nNaN,A\n',None,None),
 ('wrong\n1\n',None,None),
 ('amount,category\n123456789012345678901234567890,A\n0.1,A\n',
  {'total':'123456789012345678901234567890.1','rows':2},{'A':'123456789012345678901234567890.1'})]


def verify(workspace, version, backend, deadline):
    if version not in VERSIONS:raise Fault('ORACLE_UNKNOWN')
    results=[]
    for i,(text,expected,categories) in enumerate(CASES):
        deadline.remaining()
        fixture=Path(workspace)/'.oracle-input.csv';fixture.write_text(text)
        try:
            run=backend.run(workspace,'summary.py',fixture.name,deadline)
            if expected is None:
                ok=run['exit_code']==2 and not run['stdout'].strip()
                actual={'exit_code':run['exit_code'],'stdout':run['stdout']}
            else:
                try:actual=json.loads(run['stdout'])
                except ValueError:actual={'invalid_json':run['stdout']}
                ok=run['exit_code']==0 and isinstance(actual,dict) and all(actual.get(k)==v for k,v in expected.items())
                if version=='csv-v2':ok=ok and actual.get('categories')==categories
            results.append({'case':i,'ok':ok,'actual':actual,'expected':expected})
            if not ok:break
        finally:fixture.unlink(missing_ok=True)
    return {'ok':len(results)==len(CASES) and all(r['ok'] for r in results),'cases':results}
