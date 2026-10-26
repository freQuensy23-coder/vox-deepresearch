"""(Re)start every research service on the RunPod host. Run by scripts/deploy-prod.sh.

Python comes from backend/.venv (uv sync), Node from .node, the frontend from
frontend/build (Next standalone); deploy-prod.sh prepares all three. Settings come from config.yaml, secrets
from .env; the Codex login is the service user's ~/.codex/auth.json.
"""

import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

source = Path(__file__).resolve().parents[1]
base = Path("/workspace/vox-openclaw-dp")
home = base / "native-home"
logs = base / "migration/native-logs"
pid_file = base / "migration/native-pids.json"
node = str(source / ".node/bin/node")
GATEWAY_UID = 1001

python = str(source / "backend/.venv/bin/python")
env = {"PATH": f"{source}/backend/.venv/bin:{source}/.node/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(home)}
# The UI has no login: the proxy listens on loopback only and is reached through
# an SSH tunnel. Anonymous requests act as the existing account, whose threads
# (and research.py runs) the UI then lists.
ui_env = dict(env, DEER_FLOW_AUTH_DISABLED="1", DEER_FLOW_AUTH_DISABLED_USER_ID="c5b6a22f-648d-41b7-9291-413adeb1db4b")


def drop_to_gateway_user():
    os.setgroups([])
    os.setgid(GATEWAY_UID)
    os.setuid(GATEWAY_UID)


services = {
    "vox-relay": ([python, "local-deploy/vox_relay.py"], source, env, None),
    "tme-relay": ([python, "local-deploy/tme_relay.py"], source, env, None),
    "jev-gateway": ([python, "-m", "jev_gateway"], source / "jev-gateway", env, None),
    "jev-bridge": ([python, "local-deploy/jev_bridge.py"], source, env, None),
    "gateway": (
        [python, "-m", "uvicorn", "app.gateway.app:app", "--host", "127.0.0.1", "--port", "18091"],
        source / "backend",
        ui_env,
        drop_to_gateway_user,
    ),
    "frontend": ([node, "server.js"], source / "frontend/build", dict(ui_env, NODE_ENV="production", HOSTNAME="127.0.0.1", PORT="3000"), None),
    "proxy": (["/usr/sbin/nginx", "-c", str(source / "local-deploy/runpod-nginx.conf")], source, env, None),
}


def ancestors():
    pid, chain = os.getpid(), set()
    while pid > 1:
        chain.add(pid)
        pid = int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[1])
    return chain


def ours():
    """Processes started from this checkout (any previous launch, tracked or not),
    except this launcher and the deploy shell that runs it. The Next server renames
    itself to `next-server`, so it is found by its working directory, the build
    folder, which a deploy replaces (the old one then reads as `... (deleted)`)."""
    skip = ancestors()
    build = str(source / "frontend/build")
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit() and int(entry.name) not in skip:
            try:
                if str(source) in (entry / "cmdline").read_bytes().decode(errors="replace") or os.readlink(entry / "cwd").startswith(build):
                    yield int(entry.name)
            except OSError:
                pass


def stop_all():
    for pid in ours():
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    for _ in range(100):
        if not list(ours()):
            return
        time.sleep(0.1)
    for pid in ours():
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


logs.mkdir(parents=True, exist_ok=True)
home.mkdir(exist_ok=True)
os.chown(home, GATEWAY_UID, GATEWAY_UID)
codex_login = home / ".codex/auth.json"
if not codex_login.exists():
    # The Codex login used to live in .deployment/; keep using that account.
    codex_login.parent.mkdir(exist_ok=True)
    shutil.copyfile(source / ".deployment/codex-auth.json", codex_login)
    for path in (codex_login.parent, codex_login):
        os.chown(path, GATEWAY_UID, GATEWAY_UID)
    os.chmod(codex_login, 0o600)
stop_all()

# Runtime data lives in backend/.deer-flow, as in a local checkout. It used to be
# runtime/ behind a symlink, which the trace writers reject.
deer_flow = source / "backend/.deer-flow"
if deer_flow.is_symlink():
    deer_flow.unlink()
if (source / "runtime").is_dir() and not deer_flow.exists():
    (source / "runtime").rename(deer_flow)

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
