# Independent executor candidate — 12 October 2026

**Verdict: NOT READY for a remote pilot.** Local control tests are qualified;
rootless runtime and remote delivery still require actual provider execution.
This repository contains no Forge Core imports and does not repair production.

## Inputs and fixes

Continues the supplied Prototype and Qualification ZIPs. Sample CSV code is reused
from the existing product. Immutable `project_id`/`task_id`, current artifact hash
for UPDATE, project lock, SQLite durable phase/result, restart deadline, deterministic
ZIP packaging, crash cleanup and fetch-without-execution replace the old runner's
missing boundaries. A failure does not advance the project head. Completed repeats
return the same bytes. A saved VERIFIED operation promotes atomically on recovery
without rerunning the application. Interrupted pre-verification work may be tested
again ONLY within isolation; there are no permitted external application effects.

## Local checks

```bash
python3 -m unittest -v test_executor
python3 qualify_cycle.py --fixture --root evidence/new-local-cycle
```

The explicit fixture backend executes known benign test programs without OS
isolation. It returns `VERIFIED_TEST_FIXTURE`, never `VERIFIED_ARTIFACT`; worker.py
cannot select it. Local tests must never be relabelled as remote qualification.

## Actual runtime interface

On a separately authorized nonproduction Linux host with qualified ROOTLESS Podman
and a locally present immutable Python image ID:

```bash
python3 -I worker.py --root /srv/independent-executor/state --image sha256:ACTUAL_IMAGE_ID
```

Send a single JSON object on stdin. Supported actions:

- SUBMIT: `{"action":"SUBMIT","task":{...}}`; schema demonstrated in sample_tasks.py.
- FETCH: `{"action":"FETCH","project_id":"csv-pilot","operation_id":"64_hex_id"}`.
- STATUS: same identity fields plus `"action":"STATUS"`.

Use a forced authenticated SSH command or a trusted equivalent transport; root
path/image are operator configuration, not user-request arguments. Secrets stay
outside the candidate runtime. Do not open a public unauthenticated service.

Rootless Podman requires cgroup v2 and seccomp. The behavior probe checks uid,
capabilities, no-new-privileges, filesystem canaries, cgroup CPU/RAM/PID limits,
only-loopback interface and blocked TEST-NET egress. Missing/failed checks stop the
operation. There is no subprocess/cwd fallback. Candidate receives read-only /work
and runtime, a 16 MiB scratch tmpfs, no network, uid 65532, 256 MiB RAM, 1 CPU,
32 PIDs. Image pulls are forbidden during tasks; image ID is fixed. Candidate
output is polled and capped at 64 KiB combined accepted stdout/stderr; this is not
a hard host disk quota. Task input is <=4 MiB/50 files. Deadline <=120 seconds;
cleanup may use a bounded additional reserve (up to 5 seconds per cleanup path).

Controller watchdog kills its own process group. Container timeout and labelled
removal kill container descendants; startup cleanup addresses only journal-owned
operations under their locks. A supervisor timeout reports OUTCOME_UNKNOWN until
STATUS/FETCH resolves whether a durable result exists. Do not blindly replay an
unknown external effect. Production candidate has no external effects capability.

Current contracts are csv-v1/csv-v2. This is a limited CSV pilot, not a universal
autonomous developer. New contract versions require controller-owned behavioral
fixtures/regressions. A product-supplied PASS marker is never a contract.

## Free remote creation profile

The prepared `.github/workflows/qualification.yml` is for a NEW dedicated PUBLIC
GitHub repository using standard ubuntu-24.04 hosted runners. Private repository
jobs are skipped at job selection to avoid unapproved paid quota use. No workflow
is activated here. Existing Forge branches/services are not changed.

1. Create an empty public test repository and authorize the connected GitHub app
   for that repository (or use an already available independent free Linux host).
2. Publish ONLY this source directory's allowlisted Python files, setup script,
   README and workflow. Do NOT publish this report, original input ZIPs, old audit
   materials, state databases, secrets, or commercial data.
3. Run the manual qualification workflow. It installs runtime only on the new
   ephemeral hosted VM, pins the observed official image digest, runs a real JSON
   worker CREATE/UPDATE/fault cycle, uploads product, then downloads it twice and
   checks behavior on a second hosted VM. If rootless/cgroups are unavailable it
   fails rather than weakening isolation.
4. Obtain run/job/source SHA and artifacts through the authenticated GitHub API.
   Compare actual downloaded hashes and inspect both jobs. Environment flags or
   script PASS alone are insufficient. No remote readiness is declared beforehand.

No public repository, remote host, paid resource, Git commit, or workflow launch
was created during this preparation. Full remote time/cost and OpenCode comparison
are unmeasured. See REPORT.md and evidence/ for the actual limits of qualification.
