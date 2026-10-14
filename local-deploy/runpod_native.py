"""Start Vox services directly in RunPod using native local command execution."""

import json
import os
import shutil
import subprocess
from pathlib import Path

os.umask(0o077)
base = Path("/workspace/vox-openclaw-dp")
source = base / "home/private/vox-deepresearch"
native = Path("/opt/vox/native-runtime")
app = native / "app"
logs = base / "migration/native-logs"
logs.mkdir(exist_ok=True)
if not Path("/app").exists():
    Path("/app").symlink_to(app)
elif Path("/app").resolve() != app.resolve():
    raise RuntimeError("Existing /app is unrelated; refusing to replace it")
for destination, target in [
    (app / "backend/.deer-flow", source / "runtime"),
    (app / "skills", source / "skills"),
]:
    if not destination.exists():
        destination.symlink_to(target)
    elif destination.resolve() != target.resolve():
        raise RuntimeError(f"Unexpected existing destination {destination}")
secret_dir = Path("/run/secrets")
secret_dir.mkdir(exist_ok=True)
os.chmod(secret_dir, 0o711)
credential = secret_dir / "codex-auth.json"
credential_temp = secret_dir / "codex-auth.json.deploy"
shutil.copyfile(source / ".deployment/codex-auth.json", credential_temp)
os.chown(credential_temp, 1001, 1001)
os.chmod(credential_temp, 0o600)
os.replace(credential_temp, credential)
interpreter = native / "usr/local/bin/python3.12"
local_python = Path("/usr/local/bin/python3.12")
if not local_python.exists():
    local_python.symlink_to(interpreter)
containers = json.loads((base / "migration/containers.json").read_text())
python = app / "backend/.venv/bin/python"
python_paths = ":".join(
    str(p)
    for p in [
        source / "backend",
        source / "backend/packages/harness",
        source / "backend/packages/extension-api",
        app / "backend/.venv/lib/python3.12/site-packages",
    ]
)
env_base = {
    "PATH": f"{native}/usr/bin:{app}/backend/.venv/bin:/usr/local/bin:/usr/bin:/bin",
    "LD_LIBRARY_PATH": str(native / "usr/local/lib"),
    "PYTHONHOME": str(native / "usr/local"),
    "PYTHONPATH": python_paths,
}
config_script = """
import yaml
from pathlib import Path
source=Path('/workspace/vox-openclaw-dp/home/private/vox-deepresearch/config.yaml')
data=yaml.safe_load(source.read_text())
data.setdefault('subagent_runtime',{}).update(max_running=32,max_queued=1000,admission_policy='queue',queue_timeout_seconds=0)
data.setdefault('subagents',{}).update(timeout_seconds=0,max_total_per_run=1000)
data['sandbox']={'use':'deerflow.sandbox.local:LocalSandboxProvider','allow_host_bash':True,'bash_command_timeout':3600}
Path('/app/config.yaml').write_text(yaml.safe_dump(data,allow_unicode=True,sort_keys=False))
"""
subprocess.run(
    [str(python), "-c", config_script],
    env={**os.environ, **env_base},
    check=True,
    timeout=20,
)
os.chown(app / "config.yaml", 1001, 1001)
pid_file = base / "migration/native-pids.json"
started = json.loads(pid_file.read_text()) if pid_file.exists() else {}
for role in ["vox-relay", "gateway", "frontend"]:
    previous = started.get(role)
    if previous and Path(f"/proc/{previous}/cmdline").exists():
        continue
    match = next(c for c in containers if c["Name"] == f"/vox-deepresearch-{role}-1")
    env = dict(
        value.split("=", 1) for value in match["Config"].get("Env", []) if "=" in value
    )
    env.update(env_base)
    env["BASH_ENV"] = "/dev/null"
    env["ENV"] = "/dev/null"
    for ssh_marker in ("SSH_CLIENT", "SSH_CONNECTION", "SSH_TTY"):
        env.pop(ssh_marker, None)
    env["HOME"] = str(base / "native-home")
    Path(env["HOME"]).mkdir(exist_ok=True)
    if role == "gateway":
        os.chown(env["HOME"], 1001, 1001)
    if role == "gateway":
        env["PYTHONPATH"] = python_paths
        env["VOX_RELAY_URL"] = "http://127.0.0.1:8083"
        env["DEER_FLOW_HOME"] = str(source / "runtime")
        env["DEER_FLOW_CONFIG_PATH"] = str(app / "config.yaml")
        command = [
            str(python),
            "-m",
            "uvicorn",
            "app.gateway.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            "18091",
        ]
        cwd = source / "backend"
    elif role == "frontend":
        command = [
            str(native / "usr/bin/node"),
            str(app / "frontend/node_modules/next/dist/bin/next"),
            "start",
            "--hostname",
            "127.0.0.1",
            "--port",
            "3000",
        ]
        cwd = app / "frontend"
    else:
        env["VOX_RELAY_HOST"] = "127.0.0.1"
        for key, value in list(env.items()):
            for old, new in [
                ("/run/secrets/vox-api-key", str(source / ".deployment/vox-api-key")),
                (
                    "/var/lib/vox-relay-adaptive-state",
                    str(source / "runtime/vox-relay-adaptive-state-v6"),
                ),
                ("/var/lib/vox-relay-state", str(source / "runtime/vox-relay-state")),
                (
                    "/app/vox-scripts",
                    str(source / "skills/public/telegram-research/scripts"),
                ),
            ]:
                value = value.replace(old, new)
            env[key] = value
        command = [str(python), str(source / "local-deploy/vox_relay.py")]
        cwd = source / "local-deploy"
    with (logs / f"{role}.log").open("ab") as output:

        def gateway_identity():
            os.setgroups([])
            os.setgid(1001)
            os.setuid(1001)

        child = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            preexec_fn=gateway_identity if role == "gateway" else None,
        )
    started[role] = child.pid
(base / "migration/native-pids.json").write_text(json.dumps(started, indent=2))
print(
    json.dumps(
        {
            "started": started,
            "sandbox_provider": "LocalSandboxProvider; native commands",
        }
    ),
    flush=True,
)

# Native Jev provider and bridge; existing consumer credentials stay private.
credentials = base / "home/private/.hermes/credentials"
jev_env = {**os.environ, **env_base, "HOME": str(base / "native-home")}
jev_env["PYTHONPATH"] = str(base / "home/private/jev-gateway") + ":" + python_paths
jev_env["JEV_GATEWAY_DATABASE"] = str(base / "native-home/jev-gateway.sqlite3")
for filename in ("jev-gateway.env", "opencode-go.env"):
    for line in (credentials / filename).read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            jev_env[key] = value.strip().strip('"').strip("'")
bridge_script = """
import importlib.util, os
from pathlib import Path
path=Path('/workspace/vox-openclaw-dp/home/private/vox-deepresearch/local-deploy/jev_bridge.py')
spec=importlib.util.spec_from_file_location('native_jev_bridge',path)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
server=m.InternalServer(('127.0.0.1',8084),m.Handler)
server.key=os.environ['JEV_GATEWAY_API_KEY']
server.serve_forever()
"""
for role, command in [
    ("jev-gateway", [str(python), "-m", "jev_gateway"]),
    ("jev-bridge", [str(python), "-c", bridge_script]),
]:
    previous = started.get(role)
    if previous and Path(f"/proc/{previous}/cmdline").exists():
        continue
    with (logs / f"{role}.log").open("ab") as output:
        child = subprocess.Popen(
            command,
            cwd=source,
            env=jev_env,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    started[role] = child.pid
pid_file.write_text(json.dumps(started, indent=2), encoding="utf-8")
print(json.dumps({"native_services": list(started)}), flush=True)
