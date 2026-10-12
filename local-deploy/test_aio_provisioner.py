import io
import json
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import aio_provisioner as p


class FakeDocker:
    def __init__(self):
        self.containers = {}
        self.calls = []
        self.next_id = 1
        self.failure = None

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if self.failure:
            raise self.failure
        args = argv[3:]
        stdout, stderr, code = "", "", 0
        if args[0] == "ps":
            stdout = "\n".join(c["Id"] for c in self.containers.values())
        elif args[:2] == ["container", "inspect"]:
            target = args[2]
            matches = [c for name, c in self.containers.items() if target in (name, c["Id"])]
            if matches:
                stdout = json.dumps(matches)
            else:
                code, stderr = 1, "Error: No such container: " + target
        elif args[0] == "run":
            name = args[args.index("--name") + 1]
            labels = dict(args[i + 1].split("=", 1) for i, arg in enumerate(args) if arg == "--label")
            cid = f"{self.next_id:064x}"
            self.next_id += 1
            mounts = []
            for index, value in enumerate(args):
                if value == "--mount":
                    spec = dict(item.split("=", 1) if "=" in item else (item, "true") for item in args[index + 1].split(","))
                    mounts.append({"Source": spec["src"], "Destination": spec["dst"], "RW": "readonly" not in spec})
            envs = [args[index + 1] for index, value in enumerate(args) if value == "--env"]
            self.containers[name] = {"Id": cid, "Name": "/" + name, "Config": {"Labels": labels, "Env": envs}, "Mounts": mounts, "State": {"Running": True}, "NetworkSettings": {"Ports": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49123"}]}}}
            stdout = cid
        elif args[:2] == ["rm", "-f"]:
            target = args[2]
            self.containers = {n: c for n, c in self.containers.items() if c["Id"] != target}
        else:
            raise AssertionError(args)
        return subprocess.CompletedProcess(argv, code, stdout, stderr)


class ProvisionerTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(p, "aio_provisioner implementation is missing")
        self.temp = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "skills").mkdir()
        (self.root / ".deployment").mkdir()
        (self.root / ".deployment/vox-api-key").write_text("vox-test-key")
        self.docker = FakeDocker()
        self.patch = patch.object(p.subprocess, "run", self.docker)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.broker = p.Provisioner(self.root)
        self.payload = {"sandbox_id": "sandbox-A", "user_id": "user-A", "thread_id": "thread-A"}

    def assert_error(self, status, callback):
        with self.assertRaises(p.APIError) as caught:
            callback()
        self.assertEqual(caught.exception.status, status)

    def test_fixed_docker_policy_and_response(self):
        result = self.broker.create(self.payload)
        self.assertEqual(result, {"sandbox_id": "sandbox-A", "sandbox_url": "http://127.0.0.1:49123", "status": "Running", "max_shell_sessions": 10})
        argv, kwargs = next(c for c in self.docker.calls if c[0][3] == "run")
        self.assertEqual(argv[:3], [p.DOCKER, "--host", "unix:///run/user/1001/docker.sock"])
        for flag, value in [("--memory", "1g"), ("--cpus", "1"), ("--pids-limit", "256"), ("--security-opt", "no-new-privileges"), ("--cap-drop", "ALL"), ("--network", "vox-research-execution"), ("--publish", "127.0.0.1::8080")]:
            self.assertEqual(argv[argv.index(flag) + 1], value)
        self.assertIn("enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:1.11.0", argv)
        mounts = [argv[i + 1] for i, x in enumerate(argv) if x == "--mount"]
        self.assertEqual(mounts, [f"type=bind,src={self.root}/runtime/users/user-A/threads/thread-A/user-data,dst=/mnt/user-data", f"type=bind,src={self.root}/skills,dst=/mnt/skills,readonly", f"type=bind,src={self.root}/runtime/vox-admission,dst=/var/lib/vox-admission"])
        envs = [argv[i + 1] for i, x in enumerate(argv) if x == "--env"]
        self.assertIn("HOME=/root", envs)
        self.assertIn("MAX_SHELL_SESSIONS=10", envs)
        self.assertTrue((self.root / "runtime/users/user-A/threads/thread-A/user-data").is_dir())
        for _, options in self.docker.calls:
            self.assertGreater(options["timeout"], 0)
            self.assertLessEqual(options["timeout"], 20)
            self.assertFalse(options.get("shell", False))

    def test_admission_directory_is_shared_across_threads(self):
        self.broker.create(self.payload)
        self.broker.create({**self.payload, "sandbox_id": "sandbox-B", "thread_id": "thread-B"})
        runs = [c[0] for c in self.docker.calls if c[0][3] == "run"]
        mount = f"type=bind,src={self.root}/runtime/vox-admission,dst=/var/lib/vox-admission"
        for argv in runs:
            self.assertIn(mount, argv)
            self.assertIn("VOX_ADMISSION_DIR=/var/lib/vox-admission", argv)
        self.assertTrue((self.root / "runtime/vox-admission").is_dir())

    def test_symlinked_admission_directory_is_rejected(self):
        (self.root / "runtime").mkdir()
        (self.root / "runtime/vox-admission").symlink_to(self.root / ".deployment", target_is_directory=True)
        self.assert_error(400, lambda: self.broker.create(self.payload))
        self.assertFalse(any(c[0][3] == "run" for c in self.docker.calls))

    def test_relay_is_the_only_vox_transport_in_new_sandboxes(self):
        broker = p.Provisioner(self.root, proxy="http://egress:3128", container_transport=True)
        broker.create(self.payload)
        argv = next(c[0] for c in self.docker.calls if c[0][3] == "run")
        envs = [argv[i + 1] for i, value in enumerate(argv) if value == "--env"]
        self.assertIn("VOX_RELAY_URL=http://vox-relay:8083", envs)
        self.assertFalse(any("PROXY=" in value and not value.startswith("NO_PROXY=") for value in envs))
        mounts = [argv[i + 1] for i, value in enumerate(argv) if value == "--mount"]
        self.assertFalse(any("api_key" in value for value in mounts))

    def test_container_transport_and_direct_server_policy(self):
        broker = p.Provisioner(self.root, container_transport=True)
        result = broker.create(self.payload)
        argv = next(c[0] for c in self.docker.calls if c[0][3] == "run")
        self.assertNotIn("--publish", argv)
        self.assertIn("/usr/local/bin/python-server", argv)
        self.assertIn("SETUID", argv)
        self.assertIn("SETGID", argv)
        self.assertIn("HOME=/root", argv)
        self.assertIn("USER=root", argv)
        self.assertEqual(result['sandbox_url'], 'http://vox-research-sandbox-sandbox-A:8080')
        self.assertIn('--workspace', argv)

    def test_standard_projection_metadata_is_accepted_without_mounting(self):
        root = '/app/backend/.deer-flow'
        mounts = [{'host_path': root + '/skills_view/public', 'container_path': '/mnt/skills/public', 'read_only': True}]
        self.broker.create({**self.payload, 'extra_mounts': mounts})
        argv = next(c[0] for c in self.docker.calls if c[0][3] == 'run')
        self.assertFalse(any('src=/app/' in arg for arg in argv))
        self.assert_error(400, lambda: self.broker.create({**self.payload, 'extra_mounts': [{'host_path': '/etc', 'container_path': '/mnt/skills/public', 'read_only': True}]}))

    def test_contract_options(self):
        self.broker.create({**self.payload, "extra_mounts": [], "skills_container_path": "/mnt/skills", "include_legacy_skills": True, "provision_lark_cli_runtime": False, "provision_lark_cli_broker": False, "max_shell_sessions": 10})

    def test_capacity_for_32_subagents_and_lead_is_forwarded_and_reported(self):
        result = self.broker.create({**self.payload, "max_shell_sessions": 33})
        self.assertEqual(result["max_shell_sessions"], 33)
        argv = next(call[0] for call in self.docker.calls if call[0][3] == "run")
        self.assertIn("MAX_SHELL_SESSIONS=33", argv)
        self.assertEqual(self.broker.get("sandbox-A")["max_shell_sessions"], 33)

    def test_shell_capacity_rejects_invalid_values_before_docker(self):
        for value in (0, -1, 66, True, "33", 33.5):
            self.assert_error(400, lambda: self.broker.create({**self.payload, "max_shell_sessions": value}))
        self.assertEqual(self.docker.calls, [])

    def test_invalid_ids_and_request_overrides_are_rejected_before_docker(self):
        for field in self.payload:
            for value in ["../escape", "a/b", "", "a" * 65, None, 4, "x\n", "ü", "--name=x"]:
                self.assert_error(400, lambda: self.broker.create({**self.payload, field: value}))
        for field, value in [("extra_mounts", [{"host_path": "/"}]), ("skills_container_path", "/etc"), ("image", "evil"), ("command", ["sh"]), ("mounts", []), ("max_shell_sessions", 66), ("provision_lark_cli_runtime", True), ("provision_lark_cli_broker", True)]:
            self.assert_error(400, lambda: self.broker.create({**self.payload, field: value}))
        self.assertEqual(self.docker.calls, [])

    def test_symlink_escape_and_cross_user_redirect_rejected(self):
        (self.root / "runtime/users/user-A/threads").mkdir(parents=True)
        (self.root / "runtime/users/user-A/threads/thread-A").symlink_to(self.root / ".deployment", target_is_directory=True)
        self.assert_error(400, lambda: self.broker.create(self.payload))
        self.assertFalse((self.root / ".deployment/user-data").exists())
        self.assertFalse(any(c[0][3] == "run" for c in self.docker.calls))

    def test_fixed_skills_and_key_symlinks_rejected(self):
        for path in [self.root / "skills"]:
            original = path.with_name(path.name + "-saved")
            path.rename(original)
            path.symlink_to(original, target_is_directory=original.is_dir())
            self.assert_error(400, lambda: self.broker.create(self.payload))
            path.unlink()
            original.rename(path)

    def test_restart_recovery_running_health_and_idempotence(self):
        first = self.broker.create(self.payload)
        restarted = p.Provisioner(self.root)
        self.assertEqual(restarted.get("sandbox-A"), first)
        self.assertEqual(restarted.list(), {"sandboxes": [first]})
        self.assertEqual(restarted.create(self.payload), first)
        self.assertEqual(sum(c[0][3] == "run" for c in self.docker.calls), 1)
        self.docker.containers[p.PREFIX + "sandbox-A"]["State"]["Running"] = False
        self.assertEqual(restarted.get("sandbox-A")["status"], "Stopped")
        self.assertEqual(restarted.list(), {"sandboxes": []})
        self.assert_error(409, lambda: restarted.create(self.payload))

    def test_stale_sandbox_policy_rejected_but_removable(self):
        self.broker.create(self.payload)
        container = self.docker.containers[p.PREFIX + "sandbox-A"]
        container["Config"]["Env"] = ["HTTPS_PROXY=http://egress:3128"]
        for callback in [lambda: self.broker.get("sandbox-A"), lambda: self.broker.create(self.payload)]:
            self.assert_error(409, callback)
        self.assertEqual(self.broker.list(), {"sandboxes": []})
        self.broker.delete("sandbox-A")
        self.assertEqual(self.docker.containers, {})

    def test_immutable_owner_and_thread(self):
        self.broker.create(self.payload)
        for field in ["user_id", "thread_id"]:
            self.assert_error(409, lambda: self.broker.create({**self.payload, field: "other"}))
        self.assertEqual(sum(c[0][3] == "run" for c in self.docker.calls), 1)

    def test_capacity_three_serialized_across_threads(self):
        def create(index):
            try:
                self.broker.create({**self.payload, "sandbox_id": f"s{index}"})
                return 200
            except p.APIError as error:
                return error.status
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(create, range(8)))
        self.assertEqual(results.count(200), 3)
        self.assertEqual(results.count(409), 5)
        self.assertEqual(len(self.docker.containers), 3)

    def test_delete_exact_owned_id_and_readback(self):
        self.broker.create(self.payload)
        cid = self.docker.containers[p.PREFIX + "sandbox-A"]["Id"]
        self.broker.delete("sandbox-A")
        self.assertIn([p.DOCKER, "--host", p.SOCKET, "rm", "-f", cid], [c[0] for c in self.docker.calls])
        self.assert_error(404, lambda: self.broker.get("sandbox-A"))
        self.assert_error(404, lambda: self.broker.delete("sandbox-A"))
        self.assertEqual(self.docker.calls[-1][0][3:5], ["container", "inspect"])

    def test_unowned_or_label_mismatched_container_never_deleted(self):
        self.broker.create(self.payload)
        container = self.docker.containers[p.PREFIX + "sandbox-A"]
        container["Config"]["Labels"] = {}
        self.assert_error(409, lambda: self.broker.delete("sandbox-A"))
        self.assert_error(409, lambda: self.broker.create(self.payload))
        self.assertEqual(self.broker.list(), {"sandboxes": []})
        self.assertFalse(any(c[0][3] == "rm" for c in self.docker.calls))

    def test_non_loopback_port_is_not_returned(self):
        self.broker.create(self.payload)
        self.docker.containers[p.PREFIX + "sandbox-A"]["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostIp"] = "0.0.0.0"
        self.assert_error(503, lambda: self.broker.get("sandbox-A"))

    def test_docker_timeout_is_bounded_and_redacted(self):
        self.docker.failure = subprocess.TimeoutExpired("secret-data", 5)
        self.assert_error(503, lambda: self.broker.get("sandbox-A"))

    def test_proxy_is_only_host_configuration(self):
        broker = p.Provisioner(self.root, proxy="http://127.0.0.1:1234")
        broker.create(self.payload)
        argv = next(c[0] for c in self.docker.calls if c[0][3] == "run")
        self.assertNotIn("HTTP_PROXY=http://127.0.0.1:1234", argv)
        self.assertNotIn("HTTPS_PROXY=http://127.0.0.1:1234", argv)
        self.assertIn("VOX_RELAY_URL=http://vox-relay:8083", argv)
        self.assert_error(400, lambda: broker.create({**self.payload, "proxy": "http://evil"}))

    def test_api_auth_and_paths(self):
        api = p.API(self.broker, "test-secret")
        for method in ["GET", "POST", "DELETE"]:
            self.assert_error(401, lambda: api.handle(method, "/api/sandboxes", None, b"{}"))
            self.assert_error(401, lambda: api.handle(method, "/api/sandboxes", "wrong", b"{}"))
        self.assertEqual(self.docker.calls, [])
        for path in ["/api/sandboxes/../x", "/api/sandboxes/%2e%2e", "/api/sandboxes/a/b", "/api/sandboxes/x?user_id=y"]:
            self.assert_error(400, lambda: api.handle("GET", path, "test-secret", b""))
        self.assert_error(400, lambda: api.handle("POST", "/api/sandboxes", "test-secret", b"invalid"))
        self.assert_error(400, lambda: api.handle("POST", "/api/sandboxes", "test-secret", b"[]"))
        self.assertEqual(api.handle("POST", "/api/sandboxes", "test-secret", json.dumps(self.payload).encode())[0], 200)
        self.assertEqual(api.handle("GET", "/api/sandboxes", "test-secret", b"")[1]["sandboxes"][0]["sandbox_id"], "sandbox-A")
        self.assertEqual(api.handle("DELETE", "/api/sandboxes/sandbox-A", "test-secret", b"")[0], 200)

    def test_http_handler_without_launching_service(self):
        class Socket:
            def __init__(self, request):
                self.input = io.BytesIO(request)
                self.output = bytearray()

            def makefile(self, *args):
                return self.input

            def sendall(self, value):
                self.output.extend(value)

            def settimeout(self, timeout):
                pass

        server = SimpleNamespace(api=p.API(self.broker, "test-secret"))
        for request, status in [
            (b"GET /api/sandboxes HTTP/1.0\r\n\r\n", b"401"),
            (b"GET /api/sandboxes HTTP/1.0\r\nX-API-Key: test-secret\r\n\r\n", b"200"),
            (b"POST /api/sandboxes HTTP/1.0\r\nX-API-Key: test-secret\r\nContent-Length: 99999\r\n\r\n", b"413"),
            (b"POST /api/sandboxes HTTP/1.0\r\nX-API-Key: test-secret\r\nTransfer-Encoding: chunked\r\n\r\n", b"400"),
            (b"GET /api/sandboxes HTTP/1.0\r\nX-API-Key: test-secret\r\nX-API-Key: test-secret\r\n\r\n", b"401"),
        ]:
            sock = Socket(request)
            p.Handler(sock, ("127.0.0.1", 1234), server)
            self.assertEqual(bytes(sock.output).split(b" ")[1], status)

    def test_main_bind_configuration_without_launching_service(self):
        with patch.object(p, "load_token", return_value="test-secret"), patch.object(p, "ThreadingHTTPServer") as server, patch.dict(p.os.environ, {}, clear=True):
            p.main()
            self.assertEqual(server.call_args.args[0], ("127.0.0.1", 18092))
            server.return_value.serve_forever.assert_called_once()
        with patch.object(p, "load_token", return_value="test-secret"), patch.object(p, "ThreadingHTTPServer") as server, patch.dict(p.os.environ, {"VOX_PROVISIONER_HOST": "127.0.0.2", "VOX_PROVISIONER_PORT": "18093"}, clear=True):
            p.main()
            self.assertEqual(server.call_args.args[0], ("127.0.0.2", 18093))

    def test_token_required_and_loaded_from_file(self):
        token = self.root / "token"
        token.write_text("test-secret\n")
        with patch.dict(p.os.environ, {"VOX_PROVISIONER_TOKEN_FILE": str(token)}, clear=True):
            self.assertEqual(p.load_token(), "test-secret")
        token.write_text("")
        with patch.dict(p.os.environ, {"VOX_PROVISIONER_TOKEN_FILE": str(token)}, clear=True):
            with self.assertRaises(ValueError):
                p.load_token()
        with patch.dict(p.os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                p.load_token()


if __name__ == "__main__":
    unittest.main()
