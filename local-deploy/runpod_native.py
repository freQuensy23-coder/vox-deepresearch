"""(Re)start every research service on the RunPod host. Run by scripts/deploy.sh.

Python comes from backend/.venv (uv sync), the frontend from frontend/build
(Next standalone, built by deploy.sh). Settings come from config.yaml, secrets
from .env; the Codex login is the service user's ~/.codex/auth.json.
"""

import json
import os
import signal
import subprocess
import time
from pathlib import Path

source = Path(__file__).resolve().parents[1]
base = Path("/workspace/vox-openclaw-dp")
home = base / "native-home"
logs = base / "migration/native-logs"
pid_file = base / "migration/native-pids.json"
node = "/opt/vox/native-runtime/usr/bin/node"
GATEWAY_UID = 1001

python = str(source / "backend/.venv/bin/python")
env = {"PATH": f"{source}/backend/.venv/bin:/usr/local/bin:/usr/bin:/bin", "HOME": str(home)}


def drop_to_gateway_user():
    os.setgroups([])
    os.setgid(GATEWAY_UID)
    os.setuid(GATEWAY_UID)


services = {
    "vox-relay": ([python, "local-deploy/vox_relay.py"], source, env, None),
    "jev-gateway": ([python, "-m", "jev_gateway"], source / "jev-gateway", env, None),
    "jev-bridge": ([python, "local-deploy/jev_bridge.py"], source, env, None),
    "gateway": (
        [python, "-m", "uvicorn", "app.gateway.app:app", "--host", "127.0.0.1", "--port", "18091"],
        source / "backend",
        env,
        drop_to_gateway_user,
    ),
    "frontend": ([node, "server.js"], source / "frontend/build", dict(env, NODE_ENV="production", HOSTNAME="127.0.0.1", PORT="3000"), None),
    "proxy": (["nginx", "-c", str(source / "local-deploy/runpod-nginx.conf")], source, env, None),
}


def stop(pid):
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    for _ in range(100):
        if not Path(f"/proc/{pid}").exists():
            return
        time.sleep(0.1)
    os.killpg(pid, signal.SIGKILL)


logs.mkdir(parents=True, exist_ok=True)
home.mkdir(exist_ok=True)
os.chown(home, GATEWAY_UID, GATEWAY_UID)
deer_flow = source / "backend/.deer-flow"
if not deer_flow.exists():
    deer_flow.symlink_to(source / "runtime")

for pid in (json.loads(pid_file.read_text()) if pid_file.exists() else {}).values():
    stop(pid)
# nginx records its master in the pid file named by runpod-nginx.conf.
nginx_pid = Path("/run/vox-migration-nginx.pid")
if nginx_pid.exists():
    try:
        os.kill(int(nginx_pid.read_text()), signal.SIGQUIT)
    except (ProcessLookupError, ValueError):
        pass
    for _ in range(100):
        if not nginx_pid.exists():
            break
        time.sleep(0.1)

started = {}
for role, (command, cwd, role_env, preexec) in services.items():
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
