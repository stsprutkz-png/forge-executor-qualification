"""TEST ONLY adapter for known benign fixtures. Never used by worker.py."""
import sys
from processes import command


class FixtureBackend:
    assurance='TEST_ONLY_NO_OS_ISOLATION'
    def __init__(self):self.calls=0
    def preflight(self,deadline):
        deadline.remaining();return {'assurance':self.assurance,'qualified':False}
    def run(self,root,script,input_name,deadline):
        self.calls+=1
        return command([sys.executable,'-I',str(root/script),str(root/input_name)],deadline,
                       env={'PATH':'/usr/bin:/bin','PYTHONDONTWRITEBYTECODE':'1'})
