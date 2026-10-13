"""One native AIO bash session per managed command; never replay submissions."""

import time

from deerflow.sandbox.command_sessions import OUTPUT_LIMIT, TERMINAL

RPC_OPTIONS = {"timeout_in_seconds": 15, "max_retries": 0}


class AioCommand:
    def __init__(self, sandbox, command, env, timeout):
        self.sandbox = sandbox
        self.client = sandbox._client
        self.session = sandbox._create_bash_session(self.client)
        self.command_id = None
        self.offset = self.stderr_offset = 0
        self._pending = None
        self._closed = False
        self._cancelled = False
        try:
            response = self.client.bash.exec(
                command=f"exec < /dev/null\n{command}",
                session_id=self.session,
                env=env or {},
                async_mode=True,
                hard_timeout=timeout,
                max_output_length=OUTPUT_LIMIT,
                request_options=RPC_OPTIONS,
            )
            data = response.data
            if not data or not isinstance(data.command_id, str):
                raise RuntimeError("command submission returned no command identity; not retried")
            self.command_id = data.command_id
            self._pending = self._result(data, data)
        except BaseException:
            sandbox._mark_session_creation_ambiguous("bash", self.session)
            try:
                self.close()
            finally:
                raise RuntimeError("command submission outcome is unknown; not retried; recycle sandbox") from None

    def _result(self, data, command):
        self.offset = getattr(data, "offset", None) or self.offset
        self.stderr_offset = getattr(data, "stderr_offset", None) or self.stderr_offset
        status = getattr(command, "status", None)
        status = {"pending": "running", "killed": "cancelled"}.get(status, status)
        if status not in TERMINAL | {"running"}:
            raise RuntimeError("unknown command status; submission was not retried")
        code = getattr(command, "exit_code", None)
        if status == "timed_out":
            code = 124
        if self._cancelled and status in TERMINAL:
            status = "cancelled"
        return {"status": status, "exit_code": code, "stdout": getattr(data, "stdout", None) or "", "stderr": getattr(data, "stderr", None) or ""}

    def poll(self, wait):
        result = self._pending or {"status": "running", "exit_code": None, "stdout": "", "stderr": ""}
        self._pending = None
        deadline = time.monotonic() + wait
        while result["status"] == "running":
            remaining = max(0.0, deadline - time.monotonic())
            response = self.client.bash.output(
                session_id=self.session,
                command_id=self.command_id,
                offset=self.offset,
                stderr_offset=self.stderr_offset,
                wait=remaining > 0,
                wait_timeout=remaining,
                request_options={"timeout_in_seconds": max(15, remaining + 5), "max_retries": 0},
            )
            data = response.data
            if not data or not data.command or data.command.command_id != self.command_id:
                raise RuntimeError("command output identity missing or mismatched")
            update = self._result(data, data.command)
            for name in ("stdout", "stderr"):
                update[name] = result[name] + update[name]
                if len(update[name]) > OUTPUT_LIMIT:
                    update[name] = update[name][:OUTPUT_LIMIT]
                    update["output_truncated"] = True
            result = update
            if time.monotonic() >= deadline or result["status"] in TERMINAL:
                break
            time.sleep(min(0.02, max(0, deadline - time.monotonic())))
        return result

    def stop(self):
        self._cancelled = True
        self.client.bash.kill(session_id=self.session, signal="SIGKILL", request_options=RPC_OPTIONS)

    def close(self):
        if not self._closed:
            # Closing the native session also kills any process still inside it.
            self.client.bash.close_session(self.session, request_options=RPC_OPTIONS)
            self._closed = True
