#!/usr/bin/env bash
# The only way to deploy to production: ships the pushed HEAD of main.
#   scripts/deploy-prod.sh
# Settings: `deploy` in config.yaml. Secrets: the root .env (shipped as is).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

setting() { backend/.venv/bin/python -c "import yaml,sys; print(yaml.safe_load(open('config.yaml'))['deploy'][sys.argv[1]])" "$1"; }
POD=$(setting pod_id)
KEY=$(eval echo "$(setting ssh_key)")
DEST=$(setting path)
REPO=$(setting repo)
NODE_VERSION=$(setting node_version)

[ "$(git branch --show-current)" = main ] || { echo "deploy: switch to main" >&2; exit 1; }
[ -z "$(git status --porcelain)" ] || { echo "deploy: commit or stash local changes" >&2; exit 1; }
git fetch -q "$REPO" main
SHA=$(git rev-parse HEAD)
[ "$SHA" = "$(git rev-parse FETCH_HEAD)" ] || { echo "deploy: HEAD is not the pushed main ($REPO)" >&2; exit 1; }

# The pod's SSH port changes on every restart; ask RunPod where it is now.
ADDRESS=$(backend/.venv/bin/python -c '
import json, sys, urllib.request
from dotenv import dotenv_values
query = "query { pod(input: {podId: \"%s\"}) { runtime { ports { ip isIpPublic privatePort publicPort } } } }" % sys.argv[1]
headers = dict(Authorization="Bearer " + dotenv_values(".env")["RUNPOD_API_KEY"])
headers["Content-Type"] = "application/json"
request = urllib.request.Request("https://api.runpod.io/graphql", json.dumps(dict(query=query)).encode(), headers)
ports = json.load(urllib.request.urlopen(request, timeout=30))["data"]["pod"]["runtime"]["ports"]
ssh = next(p for p in ports if p["privatePort"] == 22 and p["isIpPublic"])
print(ssh["ip"], ssh["publicPort"])
' "$POD")
read -r HOST PORT <<< "$ADDRESS"
remote() { ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -i "$KEY" -p "$PORT" "root@$HOST" "$1"; }

echo "deploy: $SHA to $HOST:$PORT"
remote "cat > $DEST/.env.new && chmod 600 $DEST/.env.new && mv $DEST/.env.new $DEST/.env" < .env

remote "bash -s -- $(printf '%q ' "$DEST" "$REPO" "$SHA" "$NODE_VERSION")" <<'REMOTE'
set -euo pipefail
DEST=$1 REPO=$2 SHA=$3 NODE_VERSION=$4
cd "$DEST"
git() { command git -c safe.directory="$DEST" "$@"; }
git fetch -q --depth 1 "$REPO" "$SHA"
git checkout -q -f --detach FETCH_HEAD

# Node lives in the checkout: the container disk is reset when the pod restarts.
if [ "$(.node/bin/node --version 2>/dev/null)" != "v$NODE_VERSION" ]; then
    NAME="node-v$NODE_VERSION-linux-x64"
    curl -fsSL -o "/tmp/$NAME.tar.xz" "https://nodejs.org/dist/v$NODE_VERSION/$NAME.tar.xz"
    curl -fsSL "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt" | grep " $NAME.tar.xz\$" | sed "s|$NAME.tar.xz|/tmp/$NAME.tar.xz|" | sha256sum -c -
    rm -rf .node && mkdir .node && tar -xJf "/tmp/$NAME.tar.xz" -C .node --strip-components 1 && rm "/tmp/$NAME.tar.xz"
fi
export PATH="$DEST/.node/bin:$PATH" COREPACK_ENABLE_DOWNLOAD_PROMPT=0

# Python lives in the checkout too, so the unprivileged gateway user can run it.
(cd backend && UV_PYTHON_INSTALL_DIR="$DEST/.uv-python" uv sync --frozen --no-dev --python-preference only-managed)

(
    cd frontend
    corepack pnpm install --frozen-lockfile
    rm -rf .next
    NEXT_CONFIG_BUILD_OUTPUT=standalone SKIP_ENV_VALIDATION=1 \
        DEER_FLOW_INTERNAL_GATEWAY_BASE_URL=http://127.0.0.1:18091 corepack pnpm build
    rm -rf build && cp -R .next/standalone build && cp -R .next/static build/.next/static && cp -R public build/public
)

python3 local-deploy/runpod_native.py
REMOTE

echo "deploy: checking health"
remote "for port in 8083/health 8085/health 8084/v1/models 18791/healthz 18091/health 3000/ 8000/; do
    for i in \$(seq 1 180); do curl -fsS -o /dev/null http://127.0.0.1:\$port && break; sleep 1; done
    curl -fsS -o /dev/null http://127.0.0.1:\$port && echo \"ok \$port\" || { echo \"FAILED \$port\"; exit 1; }
done"
echo "deploy: $SHA is live"
