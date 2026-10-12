"""Bounded controller commands. Process supervision is NOT an OS sandbox."""
import os
import signal
import subprocess
import tempfile
import time


class Fault(Exception):
    pass


class Deadline:
    def __init__(self, seconds, expires_at=None):
        self.end = time.monotonic() + seconds
        self.expires_at = expires_at

    def remaining(self):
        remaining = self.end - time.monotonic()
        if self.expires_at is not None:
            remaining = min(remaining, self.expires_at - time.time())
        if remaining <= 0:
            raise Fault('DEADLINE_EXCEEDED')
        return remaining


def command(argv, deadline, *, cwd=None, env=None, limit=65536):
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        deadline.remaining()
        proc = subprocess.Popen(argv, cwd=cwd, env=env, stdout=out, stderr=err,
                                stdin=subprocess.DEVNULL, start_new_session=True)
        try:
            while proc.poll() is None:
                deadline.remaining()
                if os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > limit:
                    raise Fault('OUTPUT_LIMIT')
                time.sleep(min(0.01, deadline.remaining()))
            deadline.remaining()
            if os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > limit:
                raise Fault('OUTPUT_LIMIT')
            out.seek(0); err.seek(0)
            return {'exit_code':proc.returncode,'stdout':out.read(limit).decode('utf-8',errors='replace'),
                    'stderr':err.read(limit).decode('utf-8',errors='replace')}
        finally:
            try: os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            proc.wait(timeout=2)
