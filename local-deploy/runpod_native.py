"""Start the research services natively on the RunPod host.

All service settings come from the repository's config.yaml and all secrets
from its .env. The Codex login lives in the service user's ~/.codex/auth.json.
"""

import json
import os
import subprocess
from pathlib import Path

os.umask(0o077)
base = Path("/workspace/vox-openclaw-dp")
source = base / "home/private/vox-deepresearch"
native = Path("/opt/vox/native-runtime")
app = native / "app"  # prebuilt venv and frontend; the venv expects to live at /app
home = base / "native-home"
logs = base / "migration/native-logs"
pid_file = base / "migration/native-pids.json"
GATEWAY_UID = 1001


def link(destination, target):
    if not destination.exists():
        destination.symlink_to(target)
    elif destination.resolve() != target.resolve():
        raise RuntimeError(f"Unexpected existing {destination}")


logs.mkdir(exist_ok=True)
home.mkdir(exist_ok=True)
os.chown(home, GATEWAY_UID, GATEWAY_UID)
link(Path("/app"), app)
if not Path("/usr/local/bin/python3.12").exists():
    Path("/usr/local/bin/python3.12").symlink_to(native / "usr/local/bin/python3.12")
link(source / "backend/.deer-flow", source / "runtime")

python = app / "backend/.venv/bin/python"
env = {
    "PATH": f"{native}/usr/bin:{app}/backend/.venv/bin:/usr/local/bin:/usr/bin:/bin",
    "LD_LIBRARY_PATH": str(native / "usr/local/lib"),
    "PYTHONHOME": str(native / "usr/local"),
    "PYTHONPATH": ":".join(
        str(p)
        for p in [
            source / "backend",
            source / "backend/packages/harness",
            source / "backend/packages/extension-api",
            app / "backend/.venv/lib/python3.12/site-packages",
        ]
    ),
    "HOME": str(home),
}


def drop_to_gateway_user():
    os.setgroups([])
    os.setgid(GATEWAY_UID)
    os.setuid(GATEWAY_UID)


jev_gateway_env = dict(env, JEV_GATEWAY_DATABASE=str(home / "jev-gateway.sqlite3"))
jev_gateway_env["PYTHONPATH"] = f"{base}/home/private/jev-gateway:{env['PYTHONPATH']}"
for line in (source / ".env").read_text(encoding="utf-8").splitlines():
    if line and not line.startswith("#") and "=" in line:
        key, value = line.split("=", 1)
        jev_gateway_env[key.strip()] = value.strip().strip('"').strip("'")

services = {
    "vox-relay": ([str(python), "local-deploy/vox_adaptive_relay.py"], source, env, None),
    "jev-gateway": ([str(python), "-m", "jev_gateway"], source, jev_gateway_env, None),
    "jev-bridge": ([str(python), "local-deploy/jev_bridge.py"], source, env, None),
    "gateway": (
        [str(python), "-m", "uvicorn", "app.gateway.app:app", "--host", "127.0.0.1", "--port", "18091"],
        source / "backend",
        env,
        drop_to_gateway_user,
    ),
    "proxy": (["nginx", "-c", str(source / "local-deploy/runpod-nginx.conf")], source, env, None),
    "frontend": (
        [str(native / "usr/bin/node"), str(app / "frontend/node_modules/next/dist/bin/next"), "start", "--hostname", "127.0.0.1", "--port", "3000"],
        app / "frontend",
        dict(env, NODE_ENV="production", DEER_FLOW_INTERNAL_GATEWAY_BASE_URL="http://127.0.0.1:18091"),
        None,
    ),
}

started = json.loads(pid_file.read_text()) if pid_file.exists() else {}
for role, (command, cwd, role_env, preexec) in services.items():
    previous = started.get(role)
    if previous and Path(f"/proc/{previous}/cmdline").exists():
        continue
    with (logs / f"{role}.log").open("ab") as output:
        child = subprocess.Popen(
            command,
            cwd=cwd,
            env=role_env,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            preexec_fn=preexec,
        )
    started[role] = child.pid
pid_file.write_text(json.dumps(started, indent=2), encoding="utf-8")
print(json.dumps({"started": started}), flush=True)
