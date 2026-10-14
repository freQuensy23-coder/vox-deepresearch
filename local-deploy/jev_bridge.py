import http.client
import importlib.util
import json
import re
import socketserver
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import deploy_config


MAX_BYTES = 16 * 1024 * 1024
MAX_RESPONSE = 16 * 1024 * 1024


class BridgeError(Exception):
    def __init__(self, status, code):
        self.status = status
        self.code = code


def client_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "skills/public/telegram-research/scripts/jev.py"
    )
    spec = importlib.util.spec_from_file_location("jev_client", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CLIENT = client_module()


def validate(method, path, body):
    if len(body) > MAX_BYTES:
        raise BridgeError(413, "request_too_large")
    if method == "GET" and path == "/v1/models" and not body:
        return
    parsed = urlsplit(path)
    batch = re.fullmatch(r"/v1/batches/(batch_[a-f0-9]{32})(/results)?", parsed.path)
    if (
        method == "GET"
        and batch
        and not body
        and not parsed.scheme
        and not parsed.netloc
        and not parsed.fragment
    ):
        query = parse_qs(parsed.query, keep_blank_values=True)
        if not batch[2] and not query:
            return
        if (
            batch[2]
            and set(query) <= {"offset", "limit"}
            and all(
                len(values) == 1 and re.fullmatch(r"[0-9]+", values[0])
                for values in query.values()
            )
            and int(query.get("limit", ["50"])[0]) in range(1, 1001)
        ):
            return
    if (
        method == "DELETE"
        and re.fullmatch(r"/v1/batches/batch_[a-f0-9]{32}", path)
        and not body
    ):
        return
    if method != "POST" or path not in ("/v1/systemone", "/v1/batches"):
        raise BridgeError(404, "endpoint_not_allowed")
    try:
        value = json.loads(body)
        if path == "/v1/systemone":
            CLIENT.validate_body(value)
        else:
            if (
                not isinstance(value, dict)
                or "requests" not in value
                or set(value) - {"requests", "policy"}
                or ("policy" in value and not isinstance(value["policy"], dict))
            ):
                raise ValueError()
            entries = value["requests"]
            if not isinstance(entries, list) or not 1 <= len(entries) <= 10000:
                raise ValueError()
            ids = []
            for entry in entries:
                if (
                    set(entry) != {"custom_id", "body"}
                    or not isinstance(entry["custom_id"], str)
                    or not 1 <= len(entry["custom_id"]) <= 256
                ):
                    raise ValueError()
                ids.append(entry["custom_id"])
                CLIENT.validate_body(entry["body"])
            if len(set(ids)) != len(ids):
                raise ValueError()
    except (ValueError, TypeError, KeyError, CLIENT.ClientError, RecursionError):
        raise BridgeError(422, "invalid_typed_request") from None


def forward(
    method, path, body, key, gateway, connection_factory=http.client.HTTPConnection
):
    validate(method, path, body)
    connection = connection_factory(gateway.hostname, gateway.port or 80, timeout=130)
    try:
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + key}
        connection.request(method, path, body=body or None, headers=headers)
        response = connection.getresponse()
        raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise BridgeError(502, "response_too_large")
        if response.status not in (200, 202):
            return response.status, json.dumps(
                {
                    "error": {
                        "type": "gateway_error",
                        "message": f"http_{response.status}",
                    }
                }
            ).encode()
        json.loads(raw)
        return response.status, raw
    except (OSError, http.client.HTTPException, ValueError):
        return (
            503,
            b'{"error":{"type":"gateway_unavailable","message":"Jev gateway unavailable"}}',
        )
    finally:
        connection.close()


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(10)
        super().setup()

    def log_message(self, format, *args):
        pass

    def dispatch(self):
        try:
            lengths = self.headers.get_all("Content-Length", [])
            if self.headers.get("Transfer-Encoding") or len(lengths) > 1:
                raise BridgeError(400, "invalid_framing")
            length = lengths[0] if lengths else "0"
            if not re.fullmatch(r"[0-9]+", length) or int(length) > MAX_BYTES:
                raise BridgeError(413, "request_too_large")
            body = self.rfile.read(int(length))
            if len(body) != int(length):
                raise BridgeError(400, "incomplete_request")
            status, raw = forward(self.command, self.path, body, self.server.key, self.server.gateway)
        except BridgeError as error:
            status, raw = (
                error.status,
                json.dumps({"error": {"type": error.code}}).encode(),
            )
        except (OSError, ValueError, TypeError):
            status, raw = 503, b'{"error":{"type":"bridge_unavailable"}}'
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)
        self.close_connection = True

    do_GET = dispatch
    do_POST = dispatch
    do_DELETE = dispatch


class BridgeServer(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    config = deploy_config.section("jev_bridge")
    key = deploy_config.path(config["key_file"]).read_text(encoding="utf-8").strip()
    if not key or "\n" in key or "\r" in key:
        raise ValueError("Invalid Jev gateway key")
    server = BridgeServer(("127.0.0.1", int(config["port"])), Handler)
    server.key = key
    server.gateway = urlsplit(config["gateway_url"])
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
