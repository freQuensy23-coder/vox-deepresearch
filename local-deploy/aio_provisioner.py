import hmac
import json
import os
import re
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path("/home/private/vox-deepresearch")
DOCKER = "/home/private/bin/docker"
SOCKET = "unix:///run/user/1001/docker.sock"
IMAGE = "enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:1.11.0"
PREFIX = "vox-research-sandbox-"
MANAGED = "io.vox.research.provisioner"
LABEL = "io.vox.research."
IDENTIFIER = re.compile(r"[a-zA-Z0-9_-]{1,64}\Z")
MAX_BODY = 8192


class APIError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def valid_id(value):
    return isinstance(value, str) and IDENTIFIER.fullmatch(value) is not None


def require_id(value):
    if not valid_id(value):
        raise APIError(400, "Invalid sandbox, user or thread identifier")
    return value


class Provisioner:
    def __init__(self, root=ROOT, proxy="", container_transport=False):
        self.root = Path(root)
        self.proxy = proxy
        self.container_transport = container_transport
        self.lock = threading.Lock()

    def docker(self, *args, timeout=2, missing_ok=False):
        try:
            result = subprocess.run(
                [DOCKER, "--host", SOCKET, *args],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                env={"PATH": "/home/private/bin:/usr/bin:/bin"},
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise APIError(503, "Docker unavailable or timed out") from error
        if result.returncode:
            if missing_ok and "No such container:" in result.stderr:
                return None
            raise APIError(503, "Docker operation failed")
        return result.stdout.strip()

    def inspect(self, target):
        output = self.docker("container", "inspect", target, missing_ok=True)
        if output is None:
            return None
        try:
            result = json.loads(output)
            if not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], dict):
                raise ValueError()
            return result[0]
        except (ValueError, TypeError) as error:
            raise APIError(503, "Invalid Docker response") from error

    def owned(self, container):
        labels = container.get("Config", {}).get("Labels") or {}
        sandbox_id = labels.get(LABEL + "sandbox_id")
        return (
            labels.get(MANAGED) == "1"
            and all(valid_id(labels.get(LABEL + field)) for field in ("sandbox_id", "user_id", "thread_id"))
            and container.get("Name") == "/" + PREFIX + sandbox_id
            and re.fullmatch(r"[a-f0-9]{64}", container.get("Id", "")) is not None
        )

    def lookup(self, sandbox_id):
        container = self.inspect(PREFIX + require_id(sandbox_id))
        if container is None:
            raise APIError(404, "Sandbox not found")
        if not self.owned(container):
            raise APIError(409, "Container ownership mismatch")
        return container

    def containers(self):
        ids = self.docker("ps", "-aq", "--no-trunc", "--filter", f"label={MANAGED}=1").splitlines()
        containers = []
        for container_id in ids:
            if re.fullmatch(r"[a-f0-9]{64}", container_id) is None:
                raise APIError(503, "Invalid Docker container identifier")
            container = self.inspect(container_id)
            if container is not None and self.owned(container):
                containers.append(container)
        return containers

    def current_policy(self, container):
        envs = dict(value.split("=", 1) for value in container.get("Config", {}).get("Env", []) if "=" in value)
        mounts = container.get("Mounts", [])
        return (
            envs.get("VOX_RELAY_URL") == "http://vox-relay:8083"
            and envs.get("VOX_ADMISSION_DIR") == "/var/lib/vox-admission"
            and not any(name in envs for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "VOX_API_KEY"))
            and not any(mount.get("Destination") == "/root/.config/vox/api_key" for mount in mounts)
            and any(mount.get("Source") == str(self.root / "runtime/vox-admission") and mount.get("Destination") == "/var/lib/vox-admission" and mount.get("RW") is True for mount in mounts)
        )

    def response(self, container):
        if not self.current_policy(container):
            raise APIError(409, "Sandbox transport policy changed; replace the owned sandbox")
        running = container.get("State", {}).get("Running") is True
        ports = container.get("NetworkSettings", {}).get("Ports", {}).get("8080/tcp") or []
        url = ""
        if running and self.container_transport:
            url = f"http://{container['Name'].lstrip('/')}:8080"
        elif running:
            if len(ports) != 1 or ports[0].get("HostIp") != "127.0.0.1":
                raise APIError(503, "Sandbox port is not loopback-only")
            port = ports[0].get("HostPort", "")
            if not isinstance(port, str) or not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535:
                raise APIError(503, "Invalid sandbox port")
            url = f"http://127.0.0.1:{port}"
        environment = dict(value.split("=", 1) for value in container["Config"].get("Env", []) if "=" in value)
        capacity = environment.get("MAX_SHELL_SESSIONS", "10")
        if not capacity.isascii() or not capacity.isdigit() or not 1 <= int(capacity) <= 65:
            raise APIError(503, "Invalid sandbox shell capacity")
        return {
            "sandbox_id": container["Config"]["Labels"][LABEL + "sandbox_id"],
            "sandbox_url": url,
            "status": "Running" if running else "Stopped",
            "max_shell_sessions": int(capacity),
        }

    def get(self, sandbox_id):
        with self.lock:
            return self.response(self.lookup(sandbox_id))

    def list(self):
        with self.lock:
            return {"sandboxes": [self.response(c) for c in self.containers() if c.get("State", {}).get("Running") is True and self.current_policy(c)]}

    def checked_path(self, path, directory=False, create=False):
        try:
            if path.resolve() != path or not path.is_relative_to(self.root):
                raise APIError(400, "Symlinked or escaping mount path")
            if create:
                path.mkdir(parents=True, exist_ok=True, mode=0o700)
            if path.resolve(strict=True) != path or not (path.is_dir() if directory else path.is_file()):
                raise APIError(400, "Invalid mount source")
        except (OSError, RuntimeError) as error:
            raise APIError(400, "Mount source unavailable") from error
        return path

    def validate(self, payload):
        allowed = {"sandbox_id", "user_id", "thread_id", "extra_mounts", "skills_container_path", "include_legacy_skills", "provision_lark_cli_runtime", "provision_lark_cli_broker", "max_shell_sessions"}
        if not isinstance(payload, dict) or set(payload) - allowed:
            raise APIError(400, "Unsupported request fields")
        for field in ("sandbox_id", "user_id", "thread_id"):
            require_id(payload.get(field))
        if payload.get("skills_container_path", "/mnt/skills") != "/mnt/skills":
            raise APIError(400, "Mount overrides are forbidden")
        base = '/app/backend/.deer-flow'
        user = f"{base}/users/{payload['user_id']}"
        expected = {
            '/mnt/acp-workspace': (f"{user}/threads/{payload['thread_id']}/acp-workspace", True),
            '/mnt/skills/public': (f'{base}/skills_view/public', True),
            **{f'/mnt/skills/{name}': (f'{user}/skills_view/{name}', True) for name in ('custom', 'legacy', 'integrations')},
            '/mnt/integrations/lark-cli/config': (f'{user}/integrations/lark-cli/config', True),
            '/mnt/integrations/lark-cli/config/locks': (f'{user}/integrations/lark-cli/config/locks', False),
            '/mnt/integrations/lark-cli/data': (f'{user}/integrations/lark-cli/data', False),
        }
        mounts = payload.get('extra_mounts', [])
        if not isinstance(mounts, list) or len(mounts) > len(expected):
            raise APIError(400, 'Mount overrides are forbidden')
        for mount in mounts:
            if not isinstance(mount, dict) or set(mount) != {'host_path', 'container_path', 'read_only'}:
                raise APIError(400, 'Mount overrides are forbidden')
            if expected.get(mount['container_path']) != (mount['host_path'], mount['read_only']):
                raise APIError(400, 'Mount overrides are forbidden')
        for flag in ("provision_lark_cli_runtime", "provision_lark_cli_broker"):
            if payload.get(flag, False) is not False:
                raise APIError(400, "Integration provisioning is forbidden")
        if type(payload.get("include_legacy_skills", False)) is not bool:
            raise APIError(400, "Invalid skills option")
        # Native capacity supports up to 64 subagents, plus the lead's shell.
        if type(payload.get("max_shell_sessions", 10)) is not int or not 1 <= payload.get("max_shell_sessions", 10) <= 65:
            raise APIError(400, "Shell capacity must be an integer between 1 and 65")

    def create(self, payload):
        self.validate(payload)
        sandbox_id, user_id, thread_id = (payload[k] for k in ("sandbox_id", "user_id", "thread_id"))
        with self.lock:
            existing = self.inspect(PREFIX + sandbox_id)
            if existing is not None:
                if not self.owned(existing) or any(existing["Config"]["Labels"].get(LABEL + k) != payload[k] for k in ("user_id", "thread_id")):
                    raise APIError(409, "Container ownership mismatch")
                if existing.get("State", {}).get("Running") is not True:
                    raise APIError(409, "Sandbox is stopped; delete it before recreating")
                return self.response(existing)
            if len(self.containers()) >= 3:
                raise APIError(409, "Sandbox capacity reached")
            skills = self.checked_path(self.root / "skills", directory=True)
            admission = self.checked_path(self.root / "runtime/vox-admission", directory=True, create=True)
            data = self.checked_path(self.root / "runtime/users" / user_id / "threads" / thread_id / "user-data", directory=True, create=True)
            args = [
                "run", "--detach", "--pull", "never", "--name", PREFIX + sandbox_id,
                "--network", "vox-research-execution",
                "--memory", "1g", "--memory-swap", "1g", "--cpus", "1", "--pids-limit", "256",
                "--security-opt", "no-new-privileges", "--cap-drop", "ALL",
                "--label", f"{MANAGED}=1",
            ]
            if not self.container_transport:
                args.extend(["--publish", "127.0.0.1::8080"])
            for field in ("sandbox_id", "user_id", "thread_id"):
                args.extend(["--label", f"{LABEL}{field}={payload[field]}"])
            args.extend([
                "--mount", f"type=bind,src={data},dst=/mnt/user-data",
                "--mount", f"type=bind,src={skills},dst=/mnt/skills,readonly",
                "--env", "VOX_RELAY_URL=http://vox-relay:8083",
                "--mount", f"type=bind,src={admission},dst=/var/lib/vox-admission",
                "--env", "VOX_ADMISSION_DIR=/var/lib/vox-admission",
                "--env", "HOME=/root", "--env", "USER=root", "--env", f"MAX_SHELL_SESSIONS={payload.get('max_shell_sessions', 10)}",
                "--env", "OTEL_SDK_DISABLED=true", "--env", "NO_PROXY=localhost,127.0.0.1",
                "--cap-add", "SETUID", "--cap-add", "SETGID",
                "--entrypoint", "/usr/local/bin/python-server",
            ])
            self.docker(*args, IMAGE, "--host", "0.0.0.0", "--port", "8080", "--workspace", "/mnt/user-data/workspace", timeout=15)
            container = self.lookup(sandbox_id)
            if container.get("State", {}).get("Running") is not True:
                raise APIError(503, "Sandbox did not start")
            return self.response(container)

    def delete(self, sandbox_id):
        with self.lock:
            container = self.lookup(sandbox_id)
            self.docker("rm", "-f", container["Id"], timeout=5)
            if self.inspect(container["Id"]) is not None:
                raise APIError(503, "Sandbox deletion not confirmed")
            return {"sandbox_id": sandbox_id, "status": "Deleted"}


class API:
    def __init__(self, provisioner, token):
        if not token:
            raise ValueError("A nonempty API token is required")
        self.provisioner = provisioner
        self.token = token.encode("utf-8")

    def authenticate(self, token):
        if not isinstance(token, str) or not hmac.compare_digest(self.token, token.encode("utf-8")):
            raise APIError(401, "Unauthorized")

    def handle(self, method, path, token, body):
        self.authenticate(token)
        if path == "/api/sandboxes":
            if method == "GET":
                return 200, self.provisioner.list()
            if method == "POST":
                if len(body) > MAX_BODY:
                    raise APIError(413, "Request too large")
                try:
                    payload = json.loads(body)
                except (ValueError, UnicodeError) as error:
                    raise APIError(400, "Invalid JSON") from error
                return 200, self.provisioner.create(payload)
        elif path.startswith("/api/sandboxes/"):
            sandbox_id = require_id(path[len("/api/sandboxes/"):])
            if method == "GET":
                return 200, self.provisioner.get(sandbox_id)
            if method == "DELETE":
                return 200, self.provisioner.delete(sandbox_id)
        raise APIError(404, "Endpoint not found")


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(5)
        super().setup()

    def log_message(self, format, *args):
        pass

    def dispatch(self):
        try:
            tokens = self.headers.get_all("X-API-Key", [])
            token = tokens[0] if len(tokens) == 1 else None
            self.server.api.authenticate(token)
            lengths = self.headers.get_all("Content-Length", [])
            if self.headers.get("Transfer-Encoding") is not None or len(lengths) > 1:
                raise APIError(400, "Unsupported request framing")
            raw_length = lengths[0] if lengths else "0"
            if not raw_length.isascii() or not raw_length.isdigit():
                raise APIError(400, "Invalid content length")
            if len(raw_length) > 5 or int(raw_length) > MAX_BODY:
                raise APIError(413, "Request too large")
            body = self.rfile.read(int(raw_length))
            if len(body) != int(raw_length):
                raise APIError(400, "Incomplete request")
            status, result = self.server.api.handle(self.command, self.path, token, body)
        except APIError as error:
            status, result = error.status, {"detail": str(error)}
        except (OSError, ValueError, KeyError, TypeError):
            status, result = 503, {"detail": "Provisioner unavailable"}
        data = json.dumps(result).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    do_GET = dispatch
    do_POST = dispatch
    do_DELETE = dispatch


def load_token():
    filename = os.environ.get("VOX_PROVISIONER_TOKEN_FILE")
    if not filename:
        raise ValueError("VOX_PROVISIONER_TOKEN_FILE is required")
    token = Path(filename).read_text().strip()
    if not token or "\n" in token or "\r" in token:
        raise ValueError("Token file must contain one nonempty line")
    return token


def main():
    api = API(Provisioner(proxy=os.environ.get("VOX_SANDBOX_HTTP_PROXY", ""), container_transport=os.environ.get("VOX_CONTAINER_TRANSPORT") == "1"), load_token())
    server = ThreadingHTTPServer((os.environ.get("VOX_PROVISIONER_HOST", "127.0.0.1"), int(os.environ.get("VOX_PROVISIONER_PORT", "18092"))), Handler)
    server.api = api
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
