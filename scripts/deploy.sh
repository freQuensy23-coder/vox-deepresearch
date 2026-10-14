#!/usr/bin/env bash
# The only way to deploy to production: ships the pushed HEAD of main.
#   scripts/deploy.sh
# Settings: `deploy` in config.yaml. The prod SSH key lives on the jump host.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

setting() { backend/.venv/bin/python -c "import yaml,sys; print(yaml.safe_load(open('config.yaml'))['deploy'][sys.argv[1]])" "$1"; }
JUMP=$(setting jump)
TARGET=$(setting target_ssh)
DEST=$(setting path)
REPO=$(setting repo)

[ "$(git branch --show-current)" = main ] || { echo "deploy: switch to main" >&2; exit 1; }
[ -z "$(git status --porcelain)" ] || { echo "deploy: commit or stash local changes" >&2; exit 1; }
git fetch -q "$REPO" main
SHA=$(git rev-parse HEAD)
[ "$SHA" = "$(git rev-parse FETCH_HEAD)" ] || { echo "deploy: HEAD is not the pushed main ($REPO)" >&2; exit 1; }

echo "deploy: building frontend for $SHA"
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT
(
    cd frontend
    rm -rf .next
    NEXT_CONFIG_BUILD_OUTPUT=standalone SKIP_ENV_VALIDATION=1 \
        DEER_FLOW_INTERNAL_GATEWAY_BASE_URL=http://127.0.0.1:18091 \
        python3 ../scripts/pnpm.py build
)
cp -R frontend/.next/standalone "$BUILD/build"
cp -R frontend/.next/static "$BUILD/build/.next/static"
cp -R frontend/public "$BUILD/build/public"
tar -czf "$BUILD/frontend.tgz" -C "$BUILD" build

# Run one shell command line on prod. Two hops: quote it once more so the jump
# host passes it through and prod's shell interprets it.
remote() { ssh -o BatchMode=yes "$JUMP" "$TARGET $(printf '%q' "$1")"; }

echo "deploy: uploading frontend"
remote "cat > /tmp/vox-frontend-$SHA.tgz" < "$BUILD/frontend.tgz"

echo "deploy: updating $DEST to $SHA"
remote "bash -s -- $(printf '%q ' "$DEST" "$REPO" "$SHA")" <<'REMOTE'
set -euo pipefail
DEST=$1 REPO=$2 SHA=$3
cd "$DEST"
[ -d .git ] || { git init -q; git remote add origin "$REPO"; }
git fetch -q --depth 1 origin "$SHA"
git checkout -q -f --detach FETCH_HEAD
# Python lives in the checkout so the unprivileged gateway user can run it.
(cd backend && UV_PYTHON_INSTALL_DIR="$DEST/.uv-python" uv sync --frozen --no-dev)
rm -rf frontend/build.new
mkdir frontend/build.new
tar -xzf "/tmp/vox-frontend-$SHA.tgz" -C frontend/build.new --strip-components 1
rm -rf frontend/build
mv frontend/build.new frontend/build
rm -f "/tmp/vox-frontend-$SHA.tgz"
python3 local-deploy/runpod_native.py
REMOTE

echo "deploy: checking health"
remote "for port in 8083/health 8084/v1/models 18791/healthz 18091/health 3000/ 8000/; do
    for i in \$(seq 1 60); do curl -fsS -o /dev/null http://127.0.0.1:\$port && break; sleep 1; done
    curl -fsS -o /dev/null http://127.0.0.1:\$port && echo \"ok \$port\" || { echo \"FAILED \$port\"; exit 1; }
done"
echo "deploy: $SHA is live"
