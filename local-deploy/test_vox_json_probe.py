"""Offline grammar, large-response and bounded-memory contracts."""

import json
import random
import time
import tracemalloc

import pytest

from vox_json_probe import CompletionProbe, MAX_DEPTH, TOKEN_BYTES

FINAL = b'{"stop_reason":"finished","messages":[]}'


def parse(raw, *, chunk=65536, search=True):
    probe = CompletionProbe(search)
    for pos in range(0, len(raw), chunk):
        probe.feed(raw[pos : pos + chunk])
        assert probe.buffered_bytes <= TOKEN_BYTES + 3
        assert probe.depth <= MAX_DEPTH
    return probe.finish()


@pytest.mark.parametrize("chunk", [1, 7, 65536])
@pytest.mark.parametrize(
    "raw",
    [
        FINAL,
        b'{\n"stop_reason": "finished",\n"messages": [{"text":"inside"}]\n}',
        b'{"point":0}\n{"point":50}\n' + FINAL + b"\n",
        b'{"st\\u006fp_reason":"fi\\u006eished","me\\u0073sages":[]}',
        json.dumps(
            {"stop_reason": "finished", "messages": [{"text": 'Привет 🦊 \n \\ "'}]},
            ensure_ascii=False,
        ).encode(),
        b'{"stop_reason":"finished","messages":[{"error":"nested-not-root","stop_reason":"timeout","partial":true}]}',
        b'{"stop_reason":"finished","messages":[],"error":0,"errors":[],"ok":0,"partial":false}',
        b'{"stop_reason":"finished","messages":[],"metadata":[-0,0,1,-1,1.5,-0.0001,1e+2,1E-2,true,false,null]}',
    ],
)
def test_valid_complete_frames_all_chunk_boundaries(raw, chunk):
    assert parse(raw, chunk=chunk) == "success"


@pytest.mark.parametrize(
    "raw",
    [
        b'{"wrapper":[' + FINAL + b"]}",
        b'{"messages":[],"stop_reason":"timeout"}',
        b'{"messages":{},"stop_reason":"finished"}',
        b'{"point":100}',
        b'{"stop_reason":"finished","messages":[],"partial":true}',
        b'{"stop_reason":"finished","messages":[],"incomplete":true}',
        b'{"stop_reason":"finished","messages":[],"error":{"code":1}}',
        b'{"stop_reason":"finished","messages":[],"errors":[null]}',
        b'{"stop_reason":"finished","messages":[],"ok":false}',
        b'{"stop_reason":"finished","messages":[],"success":false}',
        b'{"stop_reason":"finished","messages":[],"type":"error"}',
        b'{"detail":"synthetic error"}',
        FINAL + b'\n{"errors":"later"}',
    ],
)
def test_root_only_semantic_failure(raw):
    assert parse(raw, chunk=7) == "failure"


@pytest.mark.parametrize(
    "invalid",
    [
        b"garbage",
        b"{",
        b"[",
        b'{"a"}',
        b'{"a":}',
        b'{"a":1,}',
        b"[1,]",
        b'{"a" 1}',
        b'{"a":1 "b":2}',
        b"[1 2]",
        b"[true false]",
        b"tru",
        b"True",
        b"undefined",
        b"NaN",
        b"Infinity",
        b"-Infinity",
        b"01",
        b"-01",
        b"+1",
        b"1.",
        b".1",
        b"1e",
        b"1e+",
        b"--1",
        b'"bad\x00control"',
        b'"bad\ncontrol"',
        b'"bad\\escape"',
        b'"bad\\u12z4"',
        b'"bad\\u123"',
        b'"dangling\\',
        b'"unclosed',
        b'"bad\xffutf8"',
        b'"overlong\xc0\xaf"',
        b'"surrogate\xed\xa0\x80"',
        b"{}{}",
        b"{} {}",
        b"{]",
        b"[}",
    ],
)
def test_malformed_json_never_becomes_healthy(invalid):
    assert parse(FINAL + b"\n" + invalid, chunk=1) == "failure"


def test_large_actual_shaped_1179_message_frame_is_healthy():
    message = {
        "id": 1,
        "group_id": -1001,
        "message_url": "https://t.me/synthetic/1",
        "text": "bounded fact text " * 100,
        "date": "2026-01-01T00:00:00Z",
        "type": "message",
    }
    messages = [dict(message, id=index) for index in range(1179)]
    raw = json.dumps(
        {
            "count": 1179,
            "messages": messages,
            "stop_reason": "finished",
            "stopped_at": "2026-01-01",
        },
        separators=(",", ":"),
    ).encode()
    assert len(raw) > 1756037
    assert parse(b'{"point":100}\n' + raw) == "success"
    assert parse(raw, chunk=4093) == "success"
    assert parse(raw[:-1]) == "failure"


def test_large_response_retains_bounded_memory_and_runs_in_reasonable_time():
    # Feed over 16 MiB without building that body: no payload-sized buffers.
    chunk = b"a" * 65536
    probe = CompletionProbe(True)
    tracemalloc.start()
    started = time.perf_counter()
    try:
        probe.feed(b'{"messages":[{"text":"')
        for _ in range(256):
            probe.feed(chunk)
        probe.feed(b'"}],"stop_reason":"finished"}')
        assert probe.finish() == "success"
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 1024 * 1024, peak
    assert time.perf_counter() - started < 5


def test_large_unknown_keys_and_selected_values_remain_bounded():
    large = b"x" * 2000000
    assert (
        parse(b'{"' + large + b'":null,"stop_reason":"finished","messages":[]}')
        == "success"
    )
    assert (
        parse(b'{"stop_reason":"finished","messages":[],"error":"' + large + b'"}')
        == "failure"
    )
    assert parse(b'{"stop_reason":"' + large + b'","messages":[]}') == "failure"


def test_depth_limit_is_unassessed_not_false_success():
    raw = (
        b'{"messages":'
        + b"[" * MAX_DEPTH
        + b"0"
        + b"]" * MAX_DEPTH
        + b',"stop_reason":"finished"}'
    )
    assert parse(raw, chunk=7) == "neutral"


def test_progress_remains_semantic_and_finite():
    seen = []
    probe = CompletionProbe(True, notify=lambda: seen.append(True))
    probe.feed(
        b'{"point":0}\n{"point":10}\n{"point":10}\n{"point":5}\n{"progress":1e9999}\n'
    )
    assert len(seen) == 1
    probe.feed(FINAL)
    assert probe.finish() == "success"
    assert len(seen) == 2


def test_differential_valid_json_random_nested_values():
    rng = random.Random(1827)
    primitives = [
        None,
        True,
        False,
        0,
        1,
        -1,
        0.001,
        1e20,
        "",
        "Привет",
        '"\\\n',
        "finished",
    ]

    def value(depth=0):
        if depth > 4 or rng.randrange(3) == 0:
            return rng.choice(primitives)
        if rng.randrange(2):
            return [value(depth + 1) for _ in range(rng.randrange(4))]
        return {str(i): value(depth + 1) for i in range(rng.randrange(4))}

    for _ in range(150):
        obj = {
            "stop_reason": "finished",
            "messages": [{"payload": value()} for _ in range(rng.randrange(4))],
        }
        raw = json.dumps(
            obj, ensure_ascii=rng.choice([True, False]), indent=rng.choice([None, 2])
        ).encode()
        assert json.loads(raw) == obj
        assert parse(raw, chunk=rng.choice([1, 7, 113])) == "success"
