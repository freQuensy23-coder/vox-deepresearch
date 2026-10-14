import importlib.util
import json
from pathlib import Path

import pytest


PATH = Path(__file__).parents[2] / "skills/public/telegram-research/scripts/jev.py"


def load():
    assert PATH.exists(), "disk-first Jev client missing"
    spec = importlib.util.spec_from_file_location("jev_cli", PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def payload():
    return {
        "items": [
            {"id": "001", "state": "Technical discussion"},
            {"id": "two", "state": "Service advertisement"},
        ],
        "questions": {
            "relevant": {"type": "noul", "instructions": "Relevant?"},
            "role": {
                "type": "choice",
                "criteria": {"discussion": "Discussion", "advert": "Advert"},
            },
            "priority": {"type": "score", "criteria": ["Low", "High"]},
        },
    }


def response(body):
    return {
        "model": "jev-1.13.0",
        "usage": {"input_tokens": 2, "output_tokens": 3},
        "answers": {
            "relevant": {"type": "noul", "noul": 0.9},
            "role": {
                "type": "choice",
                "choice": "discussion",
                "confidence": 0.8,
                "probabilities": {"discussion": 0.8, "advert": 0.2},
            },
            "priority": {
                "type": "score",
                "score": 0.9,
                "confidence": 0.9,
                "legend": {"0": "Low", "1": "High"},
                "probabilities": {"0": 0.1, "1": 0.9},
            },
        },
    }


def test_evaluate_keeps_complete_ids_errors_usage_without_mutating_input():
    module = load()
    calls = []

    def request(method, path, body=None):
        calls.append(body)
        if len(calls) == 2:
            raise module.ClientError("http_503")
        return response(body)

    data = payload()
    before = json.dumps(data)
    result = module.evaluate(data, request)
    assert [row["id"] for row in result["results"]] == ["001", "two"]
    assert result["results"][0]["response"]["usage"]["input_tokens"] == 2
    assert result["results"][1]["error"] == "http_503"
    assert result["counts"] == {"ok": 1, "error": 1}
    assert json.dumps(data) == before


@pytest.mark.parametrize(
    "change", ["duplicate", "unknown_type", "bad_choice", "empty", "nan"]
)
def test_invalid_input_is_rejected_before_any_call(change):
    module = load()
    data = payload()
    if change == "duplicate":
        data["items"][1]["id"] = "001"
    elif change == "too_many":
        data["items"] = [{"id": str(i), "state": "x"} for i in range(51)]
    elif change == "unknown_type":
        data["questions"]["relevant"]["type"] = "generate"
    elif change == "bad_choice":
        data["questions"]["role"]["criteria"] = []
    elif change == "empty":
        data["items"] = []
    else:
        data["items"][0]["state"] = {"x": float("nan")}
    with pytest.raises(module.ClientError):
        module.evaluate(data, lambda *args: pytest.fail("request sent"))


@pytest.mark.parametrize(
    "change",
    ["missing", "wrong_type", "probability", "choice", "legend", "usage", "model"],
)
def test_malformed_answers_are_errors_not_classifications(change):
    module = load()
    answer = response({})
    if change == "missing":
        answer["answers"].pop("role")
    elif change == "wrong_type":
        answer["answers"]["relevant"]["type"] = "score"
    elif change == "probability":
        answer["answers"]["relevant"]["noul"] = 2
    elif change == "choice":
        answer["answers"]["role"]["choice"] = "other"
    elif change == "legend":
        answer["answers"]["priority"]["legend"] = {}
    elif change == "usage":
        answer["usage"]["input_tokens"] = -1
    else:
        answer["model"] = "not-jev"
    result = module.evaluate(payload(), lambda *args: answer)
    assert result["counts"] == {"error": 2}
    assert all(row["error"] == "invalid_response" for row in result["results"])


def test_output_exclusive_and_input_hash_bound(tmp_path):
    module = load()
    target = tmp_path / "output.json"
    module.save(target, {"a": 1})
    with pytest.raises(FileExistsError):
        module.save(target, {"a": 2})
    assert json.loads(target.read_text()) == {"a": 1}
    assert target.stat().st_mode & 0o777 == 0o600


def test_batch_submission_preserves_agent_policy_and_readback():
    module = load()
    calls = []

    def request(method, path, body=None):
        calls.append((method, path, body))
        return {"id": "batch_" + "a" * 32, "total": 2, "status": "in_progress"}

    data = payload()
    data["policy"] = {"concurrency": 8, "free_wait_seconds": 600}
    handle = module.submit(data, request)
    assert calls[0][2]["policy"] == data["policy"]
    assert [row["custom_id"] for row in calls[0][2]["requests"]] == ["001", "two"]
    assert calls[1][:2] == ("GET", "/v1/batches/" + handle["id"])
    assert handle["ids"] == ["001", "two"]


def test_batch_collection_paginates_reconciles_and_preserves_order():
    module = load()
    handle = {"id": "batch_" + "a" * 32, "ids": ["001", "two"]}
    calls = []

    def request(method, path, body=None):
        calls.append(path)
        if "/results" not in path:
            return {"id": handle["id"], "total": 2, "status": "completed"}
        offset = 0 if "offset=0" in path else 1
        return {
            "offset": offset,
            "total": 2,
            "has_more": offset == 0,
            "results": [
                {
                    "custom_id": handle["ids"][offset],
                    "status_code": 200,
                    "response": response({}),
                    "error": None,
                }
            ],
        }

    result = module.collect(handle, request)
    assert [row["custom_id"] for row in result["results"]] == handle["ids"]
    assert len(calls) == 3
    assert result["complete"] is True


def test_batch_readback_failure_keeps_persisted_handle():
    module = load()
    persisted = []

    def request(method, path, body=None):
        if method == "POST":
            return {"id": "batch_" + "a" * 32, "total": 2}
        raise module.ClientError("http_503")

    with pytest.raises(module.ClientError):
        module.submit(payload(), request, persist=persisted.append)
    assert persisted[0]["ids"] == ["001", "two"]


def test_batch_count_mismatch_fails_closed():
    module = load()
    handle = {"id": "batch_" + "a" * 32, "ids": ["001", "two"]}
    with pytest.raises(module.ClientError):
        module.collect(handle, lambda *args: {"total": 3, "status": "completed"})


def test_cancel_requires_exact_handle_and_reads_back_target():
    module = load()
    handle = {"id": "batch_" + "a" * 32, "ids": ["001"]}
    calls = []

    def request(method, path, body=None):
        calls.append((method, path))
        return {"id": handle["id"], "total": 1, "status": "cancelled"}

    result = module.cancel(handle, request)
    assert result["status"] == "cancelled"
    assert calls == [
        ("DELETE", "/v1/batches/" + handle["id"]),
        ("GET", "/v1/batches/" + handle["id"]),
    ]
    with pytest.raises(module.ClientError):
        module.cancel({"id": "../models"}, request)


def test_evaluate_does_not_skip_later_items_after_five_minutes(monkeypatch):
    from types import SimpleNamespace

    module = load()
    clock = [0]
    monkeypatch.setattr(
        module, "time", SimpleNamespace(monotonic=lambda: clock[0]), raising=False
    )
    calls = []

    def request(method, path, body):
        calls.append(body)
        clock[0] += 301
        return response(body)

    result = module.evaluate(payload(), request)
    assert len(calls) == 2
    assert result["counts"] == {"ok": 2}


def test_batch_larger_than_old_client_caps_reaches_gateway():
    module = load()
    data = payload()
    data["items"] = [{"id": str(i), "state": "x" * 6000} for i in range(65)]
    calls = []

    def request(method, path, body=None):
        calls.append((method, path, body))
        return {"id": "batch_" + "a" * 32, "total": 65, "status": "in_progress"}

    handle = module.submit(data, request)
    assert len(handle["ids"]) == 65
    assert len(calls[0][2]["requests"]) == 65
    assert "policy" not in calls[0][2]


@pytest.mark.parametrize(
    "policy",
    [
        None,
        {},
        {"allow_paid": True, "concurrency": 32},
        {"allow_paid": False, "free_wait_seconds": 0},
    ],
)
def test_batch_policy_is_transparent_to_the_gateway(policy):
    module = load()
    data = payload()
    if policy is not None:
        data["policy"] = policy
    original = json.dumps(data)
    calls = []

    def request(method, path, body=None):
        calls.append((method, path, body))
        return {"id": "batch_" + "a" * 32, "total": 2}

    module.submit(data, request)
    submitted = calls[0][2]
    assert ("policy" in submitted) == (policy is not None)
    if policy is not None:
        assert submitted["policy"] == policy
    assert json.dumps(data) == original


def test_gateway_request_fields_and_question_sizes_are_not_restricted():
    module = load()
    question_name = "x" * 129
    body = {
        "model": "jev-latest",
        "state": "Synthetic",
        "extra_request_field": "preserved",
        "questions": {
            **{
                str(i): {"type": "noul", "extra_question_field": "preserved"}
                for i in range(17)
            },
            question_name: {
                "type": "choice",
                "criteria": {str(i): str(i) for i in range(33)},
            },
            "score": {"type": "score", "criteria": [str(i) for i in range(33)]},
            "": {"type": "noul", "criteria": {"extra_criterion_field": "preserved"}},
        },
    }
    assert module.validate_body(body) is body
