import logging
from types import SimpleNamespace

import pytest
import requests

from deerflow.community.aio_sandbox import aio_sandbox_provider as aio_mod
from deerflow.community.aio_sandbox import remote_backend as remote_mod
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.sandbox.lease import SandboxLeaseManager
from test_aio_sandbox_provider import _make_provider_with_active_sandbox
from test_remote_destroy_strict import response


@pytest.mark.parametrize("value", [True, False])
def test_policy_configuration(monkeypatch, value):
    config = SimpleNamespace(sandbox=SandboxConfig(use="deerflow.community.aio_sandbox:AioSandboxProvider", destroy_on_release=value), stream_bridge=None)
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    assert provider._load_config()["destroy_on_release"] is value


@pytest.mark.parametrize("value", ["false", "true", 1, 0, None])
def test_policy_requires_boolean(monkeypatch, value):
    config = SimpleNamespace(sandbox=SandboxConfig(use="deerflow.community.aio_sandbox:AioSandboxProvider", destroy_on_release=value), stream_bridge=None)
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: config)
    with pytest.raises(ValueError, match="destroy_on_release"):
        aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)._load_config()


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    provider, sandbox, _ = _make_provider_with_active_sandbox(tmp_path, "synthetic")
    provider._config["destroy_on_release"] = True
    provider._unowned_since = {}
    provider._backend = remote_mod.RemoteSandboxBackend("http://synthetic.invalid")
    state = {"alive": True, "failure": None, "deletes": 0}

    def delete(*args, **kwargs):
        state["deletes"] += 1
        if state["failure"] == "timeout":
            raise requests.Timeout("ambiguous deletion")
        if state["failure"]:
            return response(503)
        state["alive"] = False
        return response(200, {"sandbox_id": sandbox.id, "status": "Deleted"})

    def get(url, **kwargs):
        payload = {"sandbox_id": sandbox.id, "sandbox_url": "http://synthetic.invalid", "status": "Running"}
        if url.endswith("/api/sandboxes"):
            return response(200, {"sandboxes": [payload] if state["alive"] else []})
        return response(200, payload) if state["alive"] else response(404)

    monkeypatch.setattr(remote_mod.requests, "delete", delete)
    monkeypatch.setattr(remote_mod.requests, "get", get)
    manager = SandboxLeaseManager(provider)
    yield provider, sandbox, state, manager
    manager.close()
    provider.reset()


def test_only_last_holder_destroys_and_old_release_is_noop(runtime):
    provider, sandbox, state, manager = runtime
    for owner in ("lead", "child", "upload"):
        manager.retain(owner, sandbox.id, thread_id="thread", user_id="user")
    manager.release("lead")
    manager.release("upload")
    assert state["alive"]
    sandbox.close.assert_not_called()
    manager.release("child")
    assert not state["alive"]
    assert sandbox.id not in provider._warm_pool
    manager.release("child")
    provider.release(sandbox.id)
    assert state["deletes"] == 1


@pytest.mark.parametrize("failure", ["503", "timeout"])
def test_failed_release_quarantines_and_renewal_retries_without_owner(runtime, caplog, failure):
    provider, sandbox, state, manager = runtime
    state["failure"] = failure
    info = provider._sandbox_infos[sandbox.id]
    manager.retain("lead", sandbox.id, thread_id="thread", user_id="user")
    with caplog.at_level(logging.INFO):
        manager.release("lead")
    assert state["deletes"] == 1
    assert state["alive"]
    assert f"Destroyed sandbox {sandbox.id}" not in caplog.text
    assert provider.get(sandbox.id) is None
    assert sandbox.id not in provider._warm_pool
    with pytest.raises(aio_mod.SandboxBeingDestroyedError):
        provider._register_discovered_sandbox("thread", info, user_id="user")
    assert provider._ownership.take(sandbox.id) is False
    provider._reconcile_orphans()
    assert sandbox.id not in provider._warm_pool
    manager.release("lead")
    assert state["deletes"] == 1
    state["failure"] = None
    provider._renew_owned_leases()
    assert not state["alive"]
    assert state["deletes"] == 2
    assert provider._ownership.take(sandbox.id) is True
    assert sandbox.id not in provider._local_teardown


def test_retry_budget_remains_quarantined(runtime):
    provider, sandbox, state, manager = runtime
    state["failure"] = "503"
    provider.release(sandbox.id)
    for _ in range(8):
        provider._renew_owned_leases()
        provider.release(sandbox.id)
    assert state["deletes"] == 3
    assert provider.get(sandbox.id) is None
    assert sandbox.id not in provider._warm_pool
    assert provider._ownership.take(sandbox.id) is False


def test_default_release_keeps_user_runtime_warm(runtime):
    provider, sandbox, state, manager = runtime
    provider._config.pop("destroy_on_release")
    provider.release(sandbox.id)
    assert state["deletes"] == 0
    assert state["alive"]
    assert sandbox.id in provider._warm_pool


def test_pending_cleanup_retries_during_shutdown(runtime):
    provider, sandbox, state, manager = runtime
    state["failure"] = "503"
    provider.release(sandbox.id)
    state["failure"] = None
    provider.shutdown()
    assert not state["alive"]


@pytest.mark.parametrize("async_path", [False, True])
@pytest.mark.asyncio
async def test_quarantine_blocks_create_before_backend_io(runtime, monkeypatch, async_path):
    provider, sandbox, state, manager = runtime
    state["failure"] = "503"
    provider.release(sandbox.id)
    calls = []

    def unexpected_mounts(*args, **kwargs):
        calls.append("mounts")
        raise AssertionError("quarantined creation reached mount preparation")

    monkeypatch.setattr(provider, "_get_extra_mounts", unexpected_mounts)
    with pytest.raises(aio_mod.SandboxBeingDestroyedError):
        if async_path:
            await provider._create_sandbox_async("thread", sandbox.id, user_id="user")
        else:
            provider._create_sandbox("thread", sandbox.id, user_id="user")
    assert calls == []


def test_refused_ownership_never_means_cleanup_completed(runtime, monkeypatch):
    provider, sandbox, state, manager = runtime
    claim = provider._ownership.claim
    monkeypatch.setattr(provider._ownership, "claim", lambda *a, **k: False)
    provider.release(sandbox.id)
    assert state["deletes"] == 0
    assert provider.get(sandbox.id) is None
    assert sandbox.id in provider._ephemeral_pending
    monkeypatch.setattr(provider._ownership, "claim", claim)
    provider._renew_owned_leases()
    assert state["deletes"] == 1
    assert not state["alive"]


def test_fresh_runtime_registration_preserves_thread_raw(runtime, tmp_path, monkeypatch):
    from deerflow.config.paths import Paths

    provider, sandbox, state, manager = runtime
    paths = Paths(base_dir=tmp_path)
    monkeypatch.setattr(aio_mod, "get_paths", lambda: paths)
    mounts = provider._get_thread_mounts("thread", user_id="user")
    from pathlib import Path
    raw = Path(mounts[0][0]) / "sealed.json"
    raw.write_bytes(b'{"immutable":true}')
    original = raw.read_bytes()
    info = provider._sandbox_infos[sandbox.id]
    manager.retain("old", sandbox.id, thread_id="thread", user_id="user")
    manager.release("old")
    assert provider._backend.discover(sandbox.id) is None
    provider._register_created_sandbox("thread", sandbox.id, info, user_id="user")
    fresh = provider.get(sandbox.id)
    assert fresh is not sandbox
    manager.retain("new", sandbox.id, thread_id="thread", user_id="user")
    manager.release("old")
    assert provider.get(sandbox.id) is fresh
    assert raw.read_bytes() == original
    manager.release("new")
    assert raw.read_bytes() == original


def test_publish_cannot_cross_ephemeral_reservation(runtime, monkeypatch):
    provider, sandbox, state, manager = runtime
    remove = provider._remove_tracked_sandbox

    def publish_before_detach(sandbox_id):
        with pytest.raises(aio_mod.SandboxBeingDestroyedError):
            provider._publish_ownership(sandbox_id)
        return remove(sandbox_id)

    monkeypatch.setattr(provider, "_remove_tracked_sandbox", publish_before_detach)
    provider.release(sandbox.id)
    assert state["deletes"] == 1


@pytest.mark.parametrize("iteration", range(10))
def test_inflight_delete_excludes_concurrent_release_and_renewal(runtime, monkeypatch, iteration):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from deerflow.community.aio_sandbox.ownership.memory import MemoryOwnershipStore

    provider, sandbox, state, manager = runtime
    entered = threading.Event()
    finish = threading.Event()
    delete = remote_mod.requests.delete
    peer = MemoryOwnershipStore(owner_id="peer", ttl_seconds=600)
    peer._leases = provider._ownership._leases
    peer._lock = provider._ownership._lock

    def blocked_delete(*args, **kwargs):
        entered.set()
        assert finish.wait(3)
        return delete(*args, **kwargs)

    monkeypatch.setattr(remote_mod.requests, "delete", blocked_delete)
    with ThreadPoolExecutor(max_workers=2) as executor:
        releasing = executor.submit(provider.release, sandbox.id)
        try:
            assert entered.wait(3)
            assert peer.take(sandbox.id) is False
            provider.release(sandbox.id)
            provider._renew_owned_leases()
            assert provider.get(sandbox.id) is None
        finally:
            finish.set()
        releasing.result(timeout=3)
    assert state["deletes"] == 1
    assert peer.take(sandbox.id) is True


def test_reacquisition_epoch_invalidates_stale_release(runtime, monkeypatch):
    provider, sandbox, state, manager = runtime
    reserve = provider._reserve_local_teardown

    def acquire_before_reserve(sandbox_id, still_reapable):
        provider._publish_ownership(sandbox_id)
        return reserve(sandbox_id, still_reapable)

    monkeypatch.setattr(provider, "_reserve_local_teardown", acquire_before_reserve)
    provider.release(sandbox.id)
    assert state["deletes"] == 0
    assert provider.get(sandbox.id) is sandbox
