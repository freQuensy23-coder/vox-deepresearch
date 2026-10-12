"""Concurrency and durable transitions of the shared research registry."""

import importlib.util
import json
import multiprocessing
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "skills/public/telegram-research/scripts/research_registry.py"
)
spec = importlib.util.spec_from_file_location("research_registry", SCRIPT)
registry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(registry)
PROVENANCE = {
    "source": "breadth",
    "evidence": "seed hypothesis",
    "reason": "relevant hypothesis",
}


def add_writer(path, index):
    store = registry.Registry(path)
    for j in range(12):
        source = {**PROVENANCE, "source": f"worker-{index}-{j}"}
        store.add("groups", -10001, **source)
        store.add("groups", -20000 - index * 12 - j, **source)
        store.add("keywords", "  ОБЩЕЕ\u00a0слово ", **source)
        store.add("keywords", f"word-{index}-{j}", **source)


def claim_writer(path, index, start, results):
    start.wait()
    try:
        registry.Registry(path).claim_group(
            -10001, owner=str(index), folder=str(Path(path).parent / str(index))
        )
    except registry.RegistryError:
        results.put(False)
    else:
        results.put(True)


@pytest.fixture
def store(tmp_path):
    result = registry.Registry(tmp_path / "registry.json")
    result.init()
    return result


def test_concurrent_writers_no_lost_records_or_provenance(store):
    context = multiprocessing.get_context("spawn")
    writers = [
        context.Process(target=add_writer, args=(str(store.path), i)) for i in range(8)
    ]
    for writer in writers:
        writer.start()
    for writer in writers:
        writer.join(30)
        assert writer.exitcode == 0
    data = store.snapshot()
    assert len(data["groups"]["todo"]) == 97
    assert len(data["keywords"]["todo"]) == 97
    assert (
        len(
            next(item for item in data["groups"]["todo"] if item["id"] == -10001)[
                "sources"
            ]
        )
        == 96
    )
    assert (
        len(
            next(
                item
                for item in data["keywords"]["todo"]
                if item["key"] == "общее слово"
            )["sources"]
        )
        == 96
    )


def test_competing_claims_have_one_owner(store):
    store.add("groups", -10001, **PROVENANCE)
    context = multiprocessing.get_context("spawn")
    start, results = context.Event(), context.Queue()
    writers = [
        context.Process(target=claim_writer, args=(str(store.path), i, start, results))
        for i in range(8)
    ]
    for writer in writers:
        writer.start()
    start.set()
    for writer in writers:
        writer.join(30)
        assert writer.exitcode == 0
    assert sum(results.get(timeout=2) for _ in writers) == 1
    assert len(store.snapshot()["groups"]["todo"]) == 1


def test_keyword_normalization_and_cross_list_dedup(store, tmp_path):
    store.add("keywords", "  ＷＯＲＫ\u00a0Straße  ", **PROVENANCE)
    evidence = tmp_path / "search.json"
    evidence.write_text(
        '{"status":"traversed","traversal_complete":true}', encoding="utf-8"
    )
    store.complete_keyword("work strasse", evidence=evidence)
    added = store.add(
        "keywords",
        "WORK STRASSE",
        source="depth",
        evidence="message-1",
        reason="observed wording",
    )
    assert added["state"] == "done"
    assert not added["added"]
    data = store.snapshot()["keywords"]
    assert not data["todo"]
    assert len(data["done"]) == 1
    assert len(data["done"][0]["sources"]) == 2


def test_complete_preserves_folder_sources_and_no_requeue(store, tmp_path):
    store.add("groups", "-10001", **PROVENANCE)
    folder = tmp_path / "branch"
    folder.mkdir()
    facts = folder / "facts.md"
    facts.write_text("Fact with source https://t.me/example/12", encoding="utf-8")
    todo = folder / "todo.md"
    todo.write_text(
        "Optional lead for the breadth agent, no unresolved assigned work.",
        encoding="utf-8",
    )
    store.claim_group(-10001, owner="depth", folder=folder)
    done = store.complete_group(
        -10001, owner="depth", result=facts, remaining_todo=todo
    )
    assert done["result"] == str(facts)
    assert done["folder"] == str(folder)
    assert done["remaining_todo"] == str(todo)
    assert "assignment" not in done
    assert (
        store.add(
            "groups",
            -10001,
            source="other",
            evidence="other-message",
            reason="same group",
        )["state"]
        == "done"
    )
    assert len(store.snapshot()["groups"]["done"][0]["sources"]) == 2
    assert not store.snapshot()["groups"]["todo"]


@pytest.mark.parametrize("status", ["partial", "error"])
def test_release_leaves_todo_and_allows_explicit_retry(store, tmp_path, status):
    store.add("groups", -10001, **PROVENANCE)
    store.claim_group(-10001, owner="depth", folder=tmp_path)
    with pytest.raises(registry.RegistryError):
        store.release_group(-10001, owner="other", reason="failed")
    store.release_group(
        -10001, owner="depth", reason="request incomplete", status=status
    )
    data = store.snapshot()["groups"]
    assert data["done"] == []
    assert data["todo"][0]["attempts"][0]["status"] == status
    assert "assignment" not in data["todo"][0]
    with pytest.raises(registry.RegistryError):
        store.complete_group(-10001, owner="depth", result=tmp_path / "missing.md")
    store.claim_group(-10001, owner="retry", folder=tmp_path)


@pytest.mark.parametrize("content", [None, "", "word " * 501, "Fact without source"])
def test_invalid_results_cannot_mark_done(store, tmp_path, content):
    store.add("groups", -10001, **PROVENANCE)
    store.claim_group(-10001, owner="depth", folder=tmp_path)
    result = tmp_path / "facts.md"
    if content is not None:
        result.write_text(content, encoding="utf-8")
    with pytest.raises(registry.RegistryError):
        store.complete_group(-10001, owner="depth", result=result)
    assert not store.snapshot()["groups"]["done"]
    assert store.snapshot()["groups"]["todo"][0]["assignment"]["owner"] == "depth"


def test_excluded_group_records_assessment_without_fabricated_public_url(
    store, tmp_path
):
    store.add(
        "groups", -10001, source="depth", evidence="card.json", reason="candidate"
    )
    store.claim_group(-10001, owner="depth", folder=tmp_path)
    facts = tmp_path / "facts.md"
    facts.write_text(
        "Excluded: card.json identifies a private group; content was not read.",
        encoding="utf-8",
    )
    assert (
        store.complete_group(
            -10001, owner="depth", result=facts, assessment="excluded"
        )["assessment"]
        == "excluded"
    )


@pytest.mark.parametrize(
    "saved",
    [
        None,
        '{"status":"paused"}',
        '{"traversal_complete":false}',
        '{"stop_reason":"timeout"}',
        "{",
    ],
)
def test_missing_or_incomplete_keyword_evidence_remains_todo(store, tmp_path, saved):
    store.add("keywords", "work", **PROVENANCE)
    evidence = tmp_path / "search.json"
    if saved is not None:
        evidence.write_text(saved, encoding="utf-8")
    with pytest.raises(registry.RegistryError):
        store.complete_keyword("work", evidence=evidence)
    assert len(store.snapshot()["keywords"]["todo"]) == 1


@pytest.mark.parametrize(
    "corruption",
    [
        "{",
        "{}",
        '{"version":1,"groups":{"todo":{},"done":[]},"keywords":{"todo":[],"done":[]}}',
    ],
)
def test_corrupt_registry_fails_closed_including_init(store, corruption):
    store.path.write_text(corruption, encoding="utf-8")
    with pytest.raises(registry.RegistryError):
        store.init()
    with pytest.raises(registry.RegistryError):
        store.add("groups", -10001, **PROVENANCE)
    assert store.path.read_text(encoding="utf-8") == corruption


def test_replace_failure_keeps_old_json(store, monkeypatch):
    before = store.path.read_bytes()

    def fail(*args):
        raise OSError("synthetic replace failure")

    monkeypatch.setattr(registry.os, "replace", fail)
    with pytest.raises(OSError):
        store.add("groups", -10001, **PROVENANCE)
    assert store.path.read_bytes() == before
    assert not list(store.path.parent.glob("*.tmp"))


def test_lock_inode_is_stable_across_replacements_and_init_is_idempotent(store):
    inode = store.lock_path.stat().st_ino
    store.add("groups", -10001, **PROVENANCE)
    assert store.init()["groups"]["todo"][0]["id"] == -10001
    assert store.lock_path.stat().st_ino == inode


def test_cli_shares_same_transitions(store, tmp_path, capsys):
    base = ["--registry", str(store.path)]
    registry.main(
        base
        + [
            "add-group",
            "-10001",
            "--source",
            "lead",
            "--evidence",
            "message-12",
            "--reason",
            "on topic",
        ]
    )
    assert json.loads(capsys.readouterr().out)["added"] is True
    registry.main(
        base + ["claim-group", "-10001", "--owner", "depth", "--folder", str(tmp_path)]
    )
    assert json.loads(capsys.readouterr().out)["assignment"]["owner"] == "depth"
