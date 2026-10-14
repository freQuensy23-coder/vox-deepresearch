"""Offline contracts for the telegram-research scripts (vox.py, jev.py)."""

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "skills/public/telegram-research/scripts"


def load(name):
    spec = importlib.util.spec_from_file_location(f"telegram_research_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


vox = load("vox")
jev = load("jev")


def row(mid, date, group=-1001, text=None, user=7):
    return {"id": mid, "group_id": group, "date": date, "text": text or f"text {mid}", "user_id": user, "user_alias": "u7", "message_url": f"https://t.me/g/{mid}"}


def test_search_pages_back_to_since_and_deduplicates_boundary(tmp_path, monkeypatch):
    monkeypatch.setattr(vox, "PAGE", 3)
    pages = [
        {"stop_reason": "finished", "messages": [row(9, "2026-01-30T00:00:00Z"), row(8, "2026-01-20T00:00:00Z"), row(7, "2026-01-10T00:00:00Z")]},
        {"stop_reason": "finished", "messages": [row(7, "2026-01-10T00:00:00Z"), row(6, "2026-01-05T00:00:00Z")]},
    ]
    calls = []
    monkeypatch.setattr(vox, "_call", lambda path, params: calls.append(params) or pages[len(calls) - 1])
    out = tmp_path / "s.json"
    vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01", out=str(out))
    data = json.loads(out.read_text())
    assert [m["id"] for m in data["messages"]] == [9, 8, 7, 6]
    assert data["complete"] and data["pages"] == 2 and data["groups"] == {"-1001": 4}
    assert [c["to_date"] for c in calls] == ["2026-02-01T00:00:00Z", "2026-01-10T00:00:00Z"]
    assert all(c["from_date"] == "2026-01-01T00:00:00Z" and c["order"] == "desc" for c in calls)


def test_search_resumes_from_checkpoint_after_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(vox, "PAGE", 2)
    responses = [{"stop_reason": "finished", "messages": [row(5, "2026-01-20T00:00:00Z"), row(4, "2026-01-15T00:00:00Z")]}, vox.VoxError("boom")]

    def flaky(path, params):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(vox, "_call", flaky)
    out = tmp_path / "s.json"
    with pytest.raises(vox.VoxError):
        vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01", out=str(out))
    assert json.loads(out.read_text())["cursor"] == "2026-01-15T00:00:00Z"
    seen = []
    monkeypatch.setattr(vox, "_call", lambda path, params: seen.append(params["to_date"]) or {"stop_reason": "finished", "messages": [row(3, "2026-01-02T00:00:00Z")]})
    vox.Vox().search("вахта", since="2026-01-01", out=str(out))
    assert seen == ["2026-01-15T00:00:00Z"]
    assert [m["id"] for m in json.loads(out.read_text())["messages"]] == [5, 4, 3]


def test_partial_search_continues_from_stopped_at(tmp_path, monkeypatch):
    pages = [{"stop_reason": "timeout", "stopped_at": "2026-01-12T00:00:00Z", "messages": []}, {"stop_reason": "finished", "messages": [row(1, "2026-01-03T00:00:00Z")]}]
    calls = []
    monkeypatch.setattr(vox, "_call", lambda path, params: calls.append(params["to_date"]) or pages[len(calls) - 1])
    vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01", out=str(tmp_path / "s.json"))
    assert calls == ["2026-02-01T00:00:00Z", "2026-01-12T00:00:00Z"]


def test_download_unions_frequent_words_inside_group(tmp_path, monkeypatch):
    monkeypatch.setattr(vox, "FREQUENT_WORDS", ("что", "это"))
    by_word = {"что": [row(2, "2026-01-05T00:00:00Z"), row(1, "2026-01-02T00:00:00Z")], "это": [row(3, "2026-01-07T00:00:00Z"), row(2, "2026-01-05T00:00:00Z")]}
    calls = []
    monkeypatch.setattr(vox, "_call", lambda path, params: calls.append(params) or {"stop_reason": "finished", "messages": by_word[params["query"]]})
    out = tmp_path / "m.json"
    vox.Vox().download(-1001, since="2026-01-01", until="2026-02-01", out=str(out))
    data = json.loads(out.read_text())
    assert [m["id"] for m in data["messages"]] == [1, 2, 3]
    assert data["words_done"] == ["что", "это"] and data["complete"]
    assert {c["include_groups_ids"] for c in calls} == {"-1001"}


def test_rank_scores_unique_relevant_size_and_both_sources(tmp_path):
    relevant = tmp_path / "relevant.json"
    relevant.write_text(json.dumps({"messages": [
        {"group_id": -1, "text": "a"}, {"group_id": -1, "text": "a"}, {"group_id": -1, "text": "b"},
        {"group_id": -2, "text": "c"},
    ]}))
    semantic = tmp_path / "semantic.json"
    semantic.write_text(json.dumps({"groups": [{"group_id": -2, "message_count": 100}, {"group_id": -3, "message_count": 50}]}))
    out = tmp_path / "rank.json"
    vox.Vox().rank(str(relevant), str(semantic), top=2, exclude="-9", out=str(out))
    data = json.loads(out.read_text())
    assert [g["group_id"] for g in data["groups"]] == [-2, -1]
    assert data["groups"][1]["unique_relevant_messages"] == 2
    assert data["groups"][0]["in_text_search"] and data["groups"][0]["in_semantic"]
    assert [g["group_id"] for g in data["rest"]] == [-3]


def test_relay_is_required(monkeypatch):
    monkeypatch.delenv("VOX_RELAY_URL", raising=False)
    with pytest.raises(vox.VoxError, match="VOX_RELAY_URL"):
        vox._call("/groups/-1")


def test_jev_filter_splits_by_threshold_in_fixed_format(tmp_path, monkeypatch):
    source = tmp_path / "search.json"
    source.write_text(json.dumps({"query": "вахта", "messages": [row(1, "2026-01-02T00:00:00Z", text="опыт"), row(2, "2026-01-03T00:00:00Z", text="реклама")]}))
    submitted = []
    monkeypatch.setattr(jev, "submit", lambda data: submitted.append(data) or {"id": "batch_x", "ids": [i["id"] for i in data["items"]]})
    scores = {"-1001:1": 0.9, "-1001:2": 0.1}
    monkeypatch.setattr(jev, "collect", lambda handle: {"complete": True, "results": [{"custom_id": k, "response": {"answers": {"relevant": {"noul": scores[k]}}}} for k in handle["ids"]]})
    out = tmp_path / "jev" / "relevant.json"
    jev.Jev().filter("Личный опыт?", str(source), out=str(out))
    relevant = json.loads(out.read_text())
    rejected = json.loads(out.with_name("relevant.rejected.json").read_text())
    assert [m["message_id"] for m in relevant["messages"]] == [1]
    assert [m["message_id"] for m in rejected["messages"]] == [2]
    assert set(relevant["messages"][0]) == {"group_id", "group_name", "group_alias", "message_id", "message_url", "date", "user_id", "user_alias", "text", "keywords", "jev_score"}
    assert relevant["messages"][0]["keywords"] == ["вахта"]
    assert submitted[0]["questions"]["relevant"]["instructions"] == "Личный опыт?"
    assert not out.with_name("relevant.json.batch.json").exists()
