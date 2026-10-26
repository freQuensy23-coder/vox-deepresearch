import asyncio
import fcntl
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from .models import SystemOneRequest
from .routing import ERROR, GatewayError


class Store:
    def __init__(self, path, queue_limit):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        self.lock_fd = os.open(str(self.path) + ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.lock_fd)
            raise RuntimeError("Database already owned by another worker") from None
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.fchmod(fd, 0o600)
        os.close(fd)
        self.lock = threading.RLock()
        self.queue_limit = queue_limit
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA foreign_keys=ON;
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY, created REAL NOT NULL, cancelled INTEGER NOT NULL DEFAULT 0, policy TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS items(batch_id TEXT NOT NULL REFERENCES batches(id), position INTEGER NOT NULL, custom_id TEXT NOT NULL, body TEXT NOT NULL, status TEXT NOT NULL, started REAL, status_code INTEGER, response TEXT, error TEXT, PRIMARY KEY(batch_id,position), UNIQUE(batch_id,custom_id));
            CREATE INDEX IF NOT EXISTS items_status ON items(status);
            CREATE INDEX IF NOT EXISTS items_batch_status_position ON items(batch_id, status, position);
        """)
        with self.db:
            self.db.execute("UPDATE items SET status='queued' WHERE status='running'")

    async def call(self, name, *args):
        return await asyncio.to_thread(self._call, name, args)

    def _call(self, name, args):
        with self.lock:
            return getattr(self, name)(*args)

    def insert(self, request):
        batch_id = "batch_" + uuid.uuid4().hex
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            pending = self.db.execute("SELECT COUNT(*) FROM items WHERE status IN ('queued','running')").fetchone()[0]
            if pending + len(request.requests) > self.queue_limit:
                raise GatewayError(429)
            self.db.execute("INSERT INTO batches(id,created,policy) VALUES(?,?,?)", (batch_id, time.time(), request.policy.model_dump_json()))
            self.db.executemany("INSERT INTO items(batch_id,position,custom_id,body,status) VALUES(?,?,?,?, 'queued')", ((batch_id, i, item.custom_id, item.body.model_dump_json(exclude_unset=True)) for i, item in enumerate(request.requests)))
        return self.metadata(batch_id)

    def metadata(self, batch_id):
        batch = self.db.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if batch is None:
            raise GatewayError(404)
        counts = dict(self.db.execute("SELECT status,COUNT(*) FROM items WHERE batch_id=? GROUP BY status", (batch_id,)).fetchall())
        pending = counts.get("queued", 0) + counts.get("running", 0)
        status = "cancelled" if batch["cancelled"] else "in_progress" if pending else "completed"
        return {"id": batch_id, "status": status, "total": sum(counts.values()), "completed": counts.get("completed", 0), "failed": counts.get("failed", 0), "cancelled": counts.get("cancelled", 0), "pending": pending, "created_at": batch["created"], "policy": json.loads(batch["policy"])}

    def claim(self):
        """The next queued item of the oldest batch that is below its concurrency, marked running.
        Every lookup is an index probe on (batch_id, status, position), so a claim costs the
        same however many items are queued."""
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            batches = self.db.execute("SELECT b.id,b.policy FROM batches b WHERE b.cancelled=0 AND EXISTS (SELECT 1 FROM items i WHERE i.batch_id=b.id AND i.status='queued') ORDER BY b.created").fetchall()
            for batch in batches:
                policy = json.loads(batch["policy"])
                running = self.db.execute("SELECT COUNT(*) FROM items WHERE batch_id=? AND status='running'", (batch["id"],)).fetchone()[0]
                if running >= policy["concurrency"]:
                    continue
                position = self.db.execute("SELECT position FROM items WHERE batch_id=? AND status='queued' ORDER BY position LIMIT 1", (batch["id"],)).fetchone()[0]
                self.db.execute("UPDATE items SET status='running',started=COALESCE(started,?) WHERE batch_id=? AND position=?", (time.time(), batch["id"], position))
                item = dict(self.db.execute("SELECT * FROM items WHERE batch_id=? AND position=?", (batch["id"], position)).fetchone())
                item["policy"] = policy
                return item
            return None

    def is_running(self, batch_id, position):
        row = self.db.execute("SELECT status FROM items WHERE batch_id=? AND position=?", (batch_id, position)).fetchone()
        return row is not None and row[0] == "running"

    def finish(self, batch_id, position, status_code, response, error):
        with self.db:
            self.db.execute("UPDATE items SET status=?,status_code=?,response=?,error=? WHERE batch_id=? AND position=? AND status='running'", ("completed" if status_code == 200 else "failed", status_code, json.dumps(response) if response is not None else None, json.dumps(error) if error is not None else None, batch_id, position))

    def cancel(self, batch_id):
        self.metadata(batch_id)
        with self.db:
            self.db.execute("UPDATE batches SET cancelled=1 WHERE id=?", (batch_id,))
            self.db.execute("UPDATE items SET status='cancelled',status_code=499,error=? WHERE batch_id=? AND status IN ('queued','running')", (json.dumps({"type": "cancelled", "message": "Batch cancelled."}), batch_id))
        return self.metadata(batch_id)

    def results(self, batch_id, offset, limit):
        metadata = self.metadata(batch_id)
        rows = self.db.execute("SELECT custom_id,status_code,response,error FROM items WHERE batch_id=? ORDER BY position LIMIT ? OFFSET ?", (batch_id, limit, offset)).fetchall()
        results = [{"custom_id": r["custom_id"], "status_code": r["status_code"], "response": json.loads(r["response"]) if r["response"] else None, "error": json.loads(r["error"]) if r["error"] else None} for r in rows]
        return {"results": results, "offset": offset, "limit": limit, "has_more": offset + len(results) < metadata["total"], "total": metadata["total"]}

    def close(self):
        self.db.close()
        os.close(self.lock_fd)


class BatchRunner:
    def __init__(self, store, router, settings):
        self.store = store
        self.router = router
        self.settings = settings
        self.workers = []
        self.active = {}
        self.stopping = False

    def start(self):
        self.workers = [asyncio.create_task(self.worker()) for _ in range(self.settings.batch_workers)]

    async def stop(self):
        self.stopping = True
        for task in self.workers:
            task.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)

    async def cancel(self, batch_id):
        result = await self.store.call("cancel", batch_id)
        for (active_batch, position), task in list(self.active.items()):
            if active_batch == batch_id:
                task.cancel()
        return result

    async def execute(self, item):
        batch_id, position = item["batch_id"], item["position"]
        if not await self.store.call("is_running", batch_id, position):
            return
        request = SystemOneRequest.model_validate_json(item["body"])
        for attempt in range(self.settings.batch_attempts):
            try:
                response = await self.router.evaluate(request)
            except GatewayError as error:
                transient = error.status == 429 or error.status >= 500
                if not transient or attempt == self.settings.batch_attempts - 1:
                    await self.store.call("finish", batch_id, position, error.status, None, ERROR["error"])
                    return
                # Rate limits and upstream failures are retried by the same worker, so a
                # saturated upstream slows the batch down instead of failing its items.
                await asyncio.sleep(min(self.settings.retry_interval * 2**attempt, 60))
                if not await self.store.call("is_running", batch_id, position):
                    return
            else:
                await self.store.call("finish", batch_id, position, 200, response, None)
                return

    async def worker(self):
        while not self.stopping:
            item = await self.store.call("claim")
            if item is None:
                await asyncio.sleep(self.settings.retry_interval)
                continue
            key = item["batch_id"], item["position"]
            task = asyncio.create_task(self.execute(item))
            self.active[key] = task
            try:
                await task
            except asyncio.CancelledError:
                if self.stopping:
                    raise
            finally:
                self.active.pop(key, None)
