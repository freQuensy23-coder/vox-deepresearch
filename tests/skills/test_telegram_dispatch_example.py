"""Execute the documented dispatch example against a populated real registry."""

import json
import re
import sys
from pathlib import Path

from test_research_registry import PROVENANCE, registry

ROOT = Path(__file__).resolve().parents[2]
SKILL = ROOT / "skills/public/telegram-research"


def test_example_assigns_all_relevant_groups_and_picks_up_new_discoveries(tmp_path, monkeypatch):
    store = registry.Registry(tmp_path / "registry.json")
    store.init()
    relevant = set(range(-10100, -10000))
    for gid in relevant:
        store.add("groups", gid, **PROVENANCE)
    assigned = sorted(relevant)[:13]
    for gid in assigned:
        store.claim_group(gid, owner=f"existing:{gid}", folder=str(tmp_path / "groups" / str(gid)))

    text = (SKILL / "references/dispatch.md").read_text(encoding="utf-8")
    example = re.search(r"```python\n(.*?)\n```", text, re.S).group(1)
    example = example.replace('"/mnt/user-data/workspace/telegram-research/current"', repr(str(tmp_path)))
    example = example.replace('"/mnt/skills/public/telegram-research/scripts"', repr(str(SKILL / "scripts")))
    monkeypatch.setitem(sys.modules, "research_registry", registry)
    monkeypatch.setattr(sys, "path", list(sys.path))
    exec(compile(example, "dispatch-example", "exec"), {})
    tasks = json.loads((tmp_path / "wave-001.json").read_text(encoding="utf-8"))
    assert {int(item["key"]) for item in tasks} == relevant - set(assigned)
    assert len(tasks) == 87
    rows = {row["id"]: row for row in store.snapshot()["groups"]["todo"]}
    assert all("assignment" in row for row in rows.values())
    assert all(rows[gid]["assignment"]["owner"] == f"existing:{gid}" for gid in assigned)
    for item in tasks:
        folder = rows[int(item["key"])]["assignment"]["folder"]
        assert folder in item["prompt"]
        assert str(tmp_path / "registry.json") in item["prompt"]

    discoveries = {-20001, -20002, -20003}
    for gid in discoveries:
        store.add("groups", gid, **{**PROVENANCE, "source": "depth:new-lead"})
    exec(compile(example.replace('wave-001', 'wave-002'), "dispatch-example-next-wave", "exec"), {})
    following = json.loads((tmp_path / "wave-002.json").read_text(encoding="utf-8"))
    assert {int(item["key"]) for item in following} == discoveries
