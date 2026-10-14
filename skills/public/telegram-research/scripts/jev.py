import hashlib
import http.client
import json
import math
import os
import re
import sys
import time
import urllib.parse
from collections import Counter
from pathlib import Path

import fire


MAX_BYTES = 16 * 1024 * 1024
MAX_ITEMS = 10000
MODELS = {"jev-latest", "jev-preview", "jev-1.13.0"}
BATCH_ID = re.compile(r"batch_[a-f0-9]{32}\Z")


class ClientError(Exception):
    pass


def encode(value):
    try:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ClientError("invalid_json") from None


def validate_body(body):
    if not isinstance(body, dict) or not {"model", "state", "questions"} <= set(body):
        raise ClientError("invalid_body")
    if body["model"] not in MODELS or not isinstance(body["state"], (str, dict, list)):
        raise ClientError("invalid_body")
    questions = body["questions"]
    if not isinstance(questions, dict) or not questions:
        raise ClientError("invalid_questions")
    for name, question in questions.items():
        if not isinstance(name, str) or not isinstance(question, dict):
            raise ClientError("invalid_questions")
        kind = question.get("type")
        criteria = question.get("criteria")
        if kind not in {"noul", "choice", "score"}:
            raise ClientError("invalid_questions")
        if kind == "choice" and not isinstance(criteria, dict):
            raise ClientError("invalid_questions")
        if kind == "score" and (not isinstance(criteria, list) or not criteria):
            raise ClientError("invalid_questions")
        if kind == "noul" and criteria is not None and not isinstance(criteria, dict):
            raise ClientError("invalid_questions")
    if len(encode(body)) > MAX_BYTES:
        raise ClientError("request_too_large")
    return body


def requests_for(data):
    if not isinstance(data, dict) or set(data) - {
        "items",
        "questions",
        "model",
        "policy",
    }:
        raise ClientError("invalid_input")
    if "policy" in data and not isinstance(data["policy"], dict):
        raise ClientError("invalid_policy")
    items = data.get("items")
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_ITEMS:
        raise ClientError("invalid_items")
    result = []
    seen = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != {"id", "state"}:
            raise ClientError("invalid_items")
        key = item["id"]
        if not isinstance(key, str) or not key or len(key) > 256 or key in seen:
            raise ClientError("invalid_ids")
        seen.add(key)
        body = validate_body(
            {
                "model": data.get("model", "jev-latest"),
                "state": item["state"],
                "questions": data.get("questions"),
            }
        )
        result.append({"custom_id": key, "body": body})
    return result


def probability(value):
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1


def validate_response(raw, questions):
    try:
        encode(raw)
        if raw["model"] not in MODELS or set(raw["answers"]) != set(questions):
            raise ValueError()
        usage = raw["usage"]
        if any(
            type(usage[k]) is not int or usage[k] < 0
            for k in ("input_tokens", "output_tokens")
        ):
            raise ValueError()
        for key, question in questions.items():
            answer = raw["answers"][key]
            kind = question["type"]
            if answer["type"] != kind:
                raise ValueError()
            if kind == "noul":
                if not probability(answer["noul"]):
                    raise ValueError()
                continue
            probabilities = answer["probabilities"]
            if (
                not isinstance(probabilities, dict)
                or not probabilities
                or not all(probability(v) for v in probabilities.values())
            ):
                raise ValueError()
            if not probability(answer["confidence"]) or not math.isclose(
                sum(probabilities.values()), 1, abs_tol=0.02
            ):
                raise ValueError()
            if kind == "choice":
                if (
                    set(probabilities) != set(question["criteria"])
                    or answer["choice"] not in probabilities
                    or probabilities[answer["choice"]] < max(probabilities.values())
                ):
                    raise ValueError()
            else:
                legend = {str(i): value for i, value in enumerate(question["criteria"])}
                score = answer["score"]
                if (
                    answer["legend"] != legend
                    or set(probabilities) != set(legend)
                    or type(score) not in (int, float)
                    or not math.isfinite(score)
                    or not 0 <= score <= len(legend) - 1
                ):
                    raise ValueError()
    except (KeyError, TypeError, ValueError, ClientError):
        raise ClientError("invalid_response") from None
    return raw


def request(method, path, body=None):
    url = urllib.parse.urlsplit(os.environ.get("JEV_URL", ""))
    if url.scheme != "http" or url.hostname not in {"localhost", "127.0.0.1", "::1"} or url.path not in {"", "/"}:
        raise ClientError("JEV_URL must be the local Jev bridge; it comes from sandbox.environment in config.yaml")
    connection = http.client.HTTPConnection(url.hostname, url.port or 80, timeout=140)
    try:
        connection.request(
            method,
            path,
            body=encode(body) if body is not None else None,
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        raw = response.read(MAX_BYTES + 1)
        if response.status not in (200, 202):
            raise ClientError(f"http_{response.status}")
        if len(raw) > MAX_BYTES:
            raise ClientError("response_too_large")
        return json.loads(raw)
    except (OSError, http.client.HTTPException):
        raise ClientError("transport_unavailable") from None
    except (ValueError, UnicodeError):
        raise ClientError("invalid_response") from None
    finally:
        connection.close()


def evaluate(data, call=request):
    entries = requests_for(data)
    results = []
    for entry in entries:
        row = {
            "id": entry["custom_id"],
            "status": "error",
            "response": None,
            "error": None,
        }
        try:
            row["response"] = validate_response(
                call("POST", "/v1/systemone", entry["body"]), entry["body"]["questions"]
            )
            row.update(status="ok", error=None)
        except ClientError as error:
            row["error"] = str(error)
        results.append(row)
    return {
        "heuristic_only": True,
        "total": len(results),
        "counts": dict(Counter(row["status"] for row in results)),
        "results": results,
    }


def submit(data, call=request, persist=None):
    entries = requests_for(data)
    body = {"requests": entries}
    if "policy" in data:
        body["policy"] = data["policy"]
    if len(encode(body)) > MAX_BYTES:
        raise ClientError("batch_too_large")
    metadata = call("POST", "/v1/batches", body)
    batch_id = metadata.get("id")
    if (
        not isinstance(batch_id, str)
        or not BATCH_ID.fullmatch(batch_id)
        or metadata.get("total") != len(entries)
    ):
        raise ClientError("invalid_batch_response")
    handle = {
        "id": batch_id,
        "ids": [row["custom_id"] for row in entries],
        "questions": data["questions"],
        "submission": metadata,
    }
    if persist:
        persist(handle)
    readback = call("GET", "/v1/batches/" + batch_id)
    if readback.get("id") != batch_id or readback.get("total") != len(entries):
        raise ClientError("batch_readback_mismatch")
    return handle


def collect(handle, call=request):
    batch_id = handle.get("id")
    ids = handle.get("ids")
    if (
        not isinstance(batch_id, str)
        or not BATCH_ID.fullmatch(batch_id)
        or not isinstance(ids, list)
        or not 1 <= len(ids) <= MAX_ITEMS
        or any(not isinstance(key, str) for key in ids)
        or len(set(ids)) != len(ids)
    ):
        raise ClientError("invalid_handle")
    metadata = call("GET", "/v1/batches/" + batch_id)
    if metadata.get("total") != len(ids):
        raise ClientError("batch_count_mismatch")
    rows = []
    while True:
        offset = len(rows)
        page = call("GET", f"/v1/batches/{batch_id}/results?offset={offset}&limit=1000")
        values = page.get("results")
        if (
            page.get("total") != len(ids)
            or page.get("offset") != offset
            or not isinstance(values, list)
            or not values
            or len(values) > len(ids) - offset
            or type(page.get("has_more")) is not bool
        ):
            raise ClientError("invalid_batch_page")
        rows.extend(values)
        if not page["has_more"]:
            break
        if len(rows) >= len(ids):
            raise ClientError("invalid_batch_page")
    if len(rows) != len(ids) or [row.get("custom_id") for row in rows] != ids:
        raise ClientError("batch_ids_mismatch")
    for row in rows:
        if row.get("status_code") == 200 and "questions" in handle:
            try:
                validate_response(row["response"], handle["questions"])
            except ClientError:
                row.update(
                    status_code=502, response=None, error={"type": "invalid_response"}
                )
    complete = metadata.get("status") in ("completed", "cancelled") and all(
        row.get("status_code") is not None for row in rows
    )
    return {
        "heuristic_only": True,
        "id": batch_id,
        "total": len(ids),
        "complete": complete,
        "metadata": metadata,
        "results": rows,
    }


def cancel(handle, call=request):
    batch_id = handle.get("id")
    if not isinstance(batch_id, str) or not BATCH_ID.fullmatch(batch_id):
        raise ClientError("invalid_handle")
    call("DELETE", "/v1/batches/" + batch_id)
    result = call("GET", "/v1/batches/" + batch_id)
    if result.get("id") != batch_id or result.get("status") != "cancelled":
        raise ClientError("cancellation_unverified")
    return result


def save(path, data):
    raw = encode(data)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def _run(mode, input, output):
    """Run one raw mode on an INPUT JSON file; never overwrites OUTPUT."""
    target = Path(output).resolve()
    raw = Path(input).read_bytes()
    if len(raw) > MAX_BYTES:
        raise ClientError("input_too_large")
    data = json.loads(raw)
    if target.exists():
        raise ClientError("output_exists")
    receipt = {"input": str(Path(input).resolve()), "input_sha256": hashlib.sha256(raw).hexdigest(), "mode": mode}
    save(str(target) + ".request.json", receipt)
    if mode == "evaluate":
        result = evaluate(data)
    elif mode == "submit":
        result = submit(data, persist=lambda handle: save(str(target) + ".handle.json", {**handle, **receipt}))
    elif mode == "collect":
        result = collect(data)
    else:
        result = cancel(data)
    save(target, {**result, **receipt})
    return str(target)


def _messages(paths):
    """Messages from vox.py search/download outputs, deduplicated, with the queries that found them."""
    rows = {}
    for path in paths:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        for m in data["messages"]:
            key = f"{m.get('group_id')}:{m['id']}"
            row = rows.setdefault(key, {**m, "keywords": []})
            if data.get("query") and data["query"] not in row["keywords"]:
                row["keywords"].append(data["query"])
    return rows


class Jev:
    """Typed Jev evaluations through the local Jev bridge (JEV_URL). Outputs are heuristics, not evidence."""

    def evaluate(self, input, output):
        return _run("evaluate", input, output)

    def submit(self, input, output):
        return _run("submit", input, output)

    def collect(self, input, output):
        return _run("collect", input, output)

    def cancel(self, input, output):
        return _run("cancel", input, output)

    def filter(self, instruction, *messages, threshold=0.5, out="jev/relevant.json", poll=10):
        """Score every message of vox.py outputs against INSTRUCTION; save those >= THRESHOLD.

        Writes OUT (relevant messages, fixed format) and OUT with .rejected.json (the rest,
        with scores, for review). Resumes the same Jev batch while OUT.batch.json exists.
        """
        out = Path(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        rows = _messages(messages)
        if not rows:
            raise ClientError("no_messages")
        keys = list(rows)
        questions = {"relevant": {"type": "noul", "instructions": instruction}}
        state_path = out.with_name(out.name + ".batch.json")
        state = json.loads(state_path.read_text()) if state_path.exists() else None
        if state and (state["instruction"] != instruction or state["keys"] != keys):
            raise ClientError("batch_state_mismatch: remove the .batch.json or pass another --out")
        if not state:
            handles = []
            for start in range(0, len(keys), MAX_ITEMS):
                chunk = keys[start : start + MAX_ITEMS]
                items = [{"id": key, "state": rows[key].get("text") or ""} for key in chunk]
                handles.append(submit({"items": items, "questions": questions}))
            state = {"instruction": instruction, "keys": keys, "handles": handles}
            state_path.write_text(json.dumps(state, ensure_ascii=False))
        scores = {}
        for handle in state["handles"]:
            while True:
                result = collect(handle)
                if result["complete"]:
                    break
                print(f"jev batch {handle['id']}: waiting", file=sys.stderr)
                time.sleep(poll)
            for row in result["results"]:
                answer = ((row.get("response") or {}).get("answers") or {}).get("relevant") or {}
                scores[row["custom_id"]] = answer.get("noul")
        relevant, rejected = [], []
        for key in keys:
            m = rows[key]
            record = {
                "group_id": m.get("group_id"),
                "group_name": m.get("group_name"),
                "group_alias": m.get("group_alias"),
                "message_id": m["id"],
                "message_url": m.get("message_url"),
                "date": m.get("date"),
                "user_id": m.get("user_id"),
                "user_alias": m.get("user_alias"),
                "text": m.get("text"),
                "keywords": m["keywords"],
                "jev_score": scores.get(key),
            }
            (relevant if record["jev_score"] is not None and record["jev_score"] >= threshold else rejected).append(record)
        meta = {"instruction": instruction, "threshold": threshold, "sources": [str(Path(p).resolve()) for p in messages], "errors": sum(scores.get(k) is None for k in keys)}
        out.write_text(json.dumps({**meta, "messages": relevant}, ensure_ascii=False, indent=1))
        out.with_name(out.stem + ".rejected.json").write_text(json.dumps({**meta, "messages": rejected}, ensure_ascii=False, indent=1))
        state_path.unlink()
        print(f"{len(relevant)} relevant, {len(rejected)} rejected, {meta['errors']} errors", file=sys.stderr)
        return str(out.resolve())


def main():
    try:
        fire.Fire(Jev, serialize=lambda value: value)
    except (ClientError, OSError, ValueError, TypeError) as error:
        print(str(error) if isinstance(error, ClientError) else f"local_file_error: {error}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
