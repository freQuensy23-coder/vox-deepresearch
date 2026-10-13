"""POSIX command lifetime with bounded output and process-group cancellation."""

import codecs
import os
import select
import signal
import subprocess
import threading
import time

from deerflow.sandbox.command_sessions import OUTPUT_LIMIT


class LocalCommand:
    def __init__(self, args, *, env, timeout):
        self.process = subprocess.Popen(args, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        self._lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._out = {"stdout": bytearray(), "stderr": bytearray()}
        self._truncated = False
        self._reason = None
        self._done = threading.Event()
        self._reader_stop = threading.Event()
        self._decoders = {name: codecs.getincrementaldecoder("utf-8")("replace") for name in self._out}
        self._readers = [threading.Thread(target=self._drain, args=(name, getattr(self.process, name)), daemon=True) for name in self._out]
        for reader in self._readers:
            reader.start()
        self._watcher = threading.Thread(target=self._watch, args=(timeout,), daemon=True)
        self._watcher.start()

    def _drain(self, name, stream):
        try:
            while not self._reader_stop.is_set():
                if not select.select([stream], [], [], 0.1)[0]:
                    continue
                chunk = os.read(stream.fileno(), 65536)
                if not chunk:
                    break
                with self._lock:
                    room = OUTPUT_LIMIT * 4 - len(self._out[name])
                    self._out[name].extend(chunk[:room])
                    self._truncated |= len(chunk) > room
        finally:
            stream.close()

    def _kill(self, reason):
        with self._stop_lock:
            if self._done.is_set():
                return
            self._reason = reason
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                pass
            # Kill surviving descendants even when the shell itself already exited.
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait()
            self._reader_stop.set()

    def _watch(self, timeout):
        deadline = time.monotonic() + timeout
        try:
            self.process.wait(timeout=timeout)
            for reader in self._readers:
                reader.join(max(0, deadline - time.monotonic()))
            if any(reader.is_alive() for reader in self._readers):
                self._kill("timed_out")
        except subprocess.TimeoutExpired:
            self._kill("timed_out")
        finally:
            self._reader_stop.set()
            for reader in self._readers:
                reader.join()
            self._done.set()

    def poll(self, wait):
        self._done.wait(wait)
        with self._lock:
            result = {name: self._decoders[name].decode(bytes(buf), final=self._done.is_set()) for name, buf in self._out.items()}
            for buf in self._out.values():
                buf.clear()
            result["output_truncated"] = self._truncated
            self._truncated = False
        finished = self._done.is_set()
        result.update(status=(self._reason or "completed") if finished else "running", exit_code=(124 if self._reason == "timed_out" else self.process.returncode) if finished else None)
        return result

    def stop(self):
        self._kill("cancelled")
        self._done.wait()

    def close(self):
        if not self._done.is_set():
            self.stop()
