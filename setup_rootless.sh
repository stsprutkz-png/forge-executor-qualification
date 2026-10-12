#!/usr/bin/env bash
# Run ONLY on a dedicated disposable Ubuntu hosted runner, never the Forge VPS.
set -euo pipefail
test "${GITHUB_ACTIONS:-}" = "true"
test "${RUNNER_ENVIRONMENT:-}" = "github-hosted"
test "$(id -u)" -ne 0
sudo apt-get update -qq
sudo apt-get install -y -qq podman uidmap slirp4netns
export XDG_RUNTIME_DIR="/run/user/$(id -u)"
test -d "$XDG_RUNTIME_DIR"
podman info --format=json > podman-host-info.json
image_ref="${1:-docker.io/library/python:3.12-slim}"
podman pull "$image_ref"
podman image inspect "$image_ref" > image-inspect.json
python3 - <<'PY'
import json
from pathlib import Path
image=json.loads(Path('image-inspect.json').read_text())[0]
image_id=image['Id'];image_id=image_id if image_id.startswith('sha256:') else 'sha256:'+image_id
profile={'image_id':image_id,'image_reference':image['RepoDigests'][0]}
Path('runtime-profile.json').write_text(json.dumps(profile,indent=2))
print(json.dumps(profile))
PY
