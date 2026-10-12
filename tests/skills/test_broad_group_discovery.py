import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture
def vox():
    path = Path(__file__).resolve().parents[2] / 'skills/public/telegram-research/scripts/vox.py'
    spec = importlib.util.spec_from_file_location('broad_discovery_vox', path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('limit,offset', [(100, 0), (100, 100), (1000, 0)])
def test_broad_discovery_preserves_requested_page_and_candidates(vox, monkeypatch, tmp_path, limit, offset):
    calls = []
    rows = [{'id': -1000000 - offset - index, 'title': f'Synthetic community {offset + index}'} for index in range(limit)]

    def transport(path, parameters):
        calls.append((path, parameters))
        return rows

    monkeypatch.setattr(vox, 'call', transport)
    output = tmp_path / 'candidates.json'
    vox.main(['groups', 'synthetic professional topic', '--limit', str(limit), '--offset', str(offset), '--output', str(output)])
    saved = json.loads(output.read_text())
    assert calls == [('/search/ai/raw/GROUP', {'query': 'synthetic professional topic', 'limit': limit, 'offset': offset})]
    assert saved['evidence_type'] == 'ai_discovery'
    assert saved['public_access'] == 'check_group_cards'
    assert saved['candidates'] == rows
