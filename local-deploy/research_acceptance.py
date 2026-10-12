import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from datetime import datetime
from typing import Any


SCHEMA = "external-research-acceptance/v1"
MAX_BYTES = 64 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def text(value):
    return isinstance(value, str) and bool(value.strip())


def timestamp(value):
    require(text(value), "missing timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "timestamp requires timezone")
    return parsed


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=unique_object, parse_constant=lambda value: require(False, "invalid JSON constant"))


def pointer(value, path=""):
    require(isinstance(path, str) and (not path or path.startswith("/")), "invalid JSON pointer")
    for token in path.split("/")[1:]:
        require(not re.search(r"~(?![01])", token), "invalid pointer escape")
        token = token.replace("~1", "/").replace("~0", "~")
        value = value[int(token)] if isinstance(value, list) else value[token]
    return value


def read_scoped(root, name):
    require(text(name) and ":" not in name and "\\" not in name, "invalid local path")
    relative = PurePosixPath(name)
    require(not relative.is_absolute() and ".." not in relative.parts, "path escapes scope")
    root = Path(root).resolve(strict=True)
    current = root
    for part in relative.parts:
        current = current / part
        require(not current.is_symlink(), "symlinks are not allowed")
    require(current.resolve(strict=True).is_relative_to(root), "path escapes scope")
    descriptor = os.open(current, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode), "regular file required")
        require(before.st_size <= MAX_BYTES, "file exceeds size limit")
        data = stream.read(MAX_BYTES + 1)
        after = os.fstat(stream.fileno())
    require(len(data) <= MAX_BYTES and (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns), "file changed while reading")
    return data


def load_ref(root, reference, raw=False) -> Any:
    require(isinstance(reference, dict), "hashed file reference required")
    require(set(reference) <= {"path", "sha256", "pointer"}, "unknown file reference fields")
    require(isinstance(reference.get("sha256"), str) and re.fullmatch("[0-9a-f]{64}", reference["sha256"]), "invalid sha256")
    data = read_scoped(root, reference["path"])
    require(hashlib.sha256(data).hexdigest() == reference["sha256"], "sha256 mismatch: " + reference["path"])
    if raw and "pointer" not in reference:
        return data.decode("utf-8")
    return pointer(decode(data), reference.get("pointer", ""))


def review_digest(manifest):
    body = {key: value for key, value in manifest.items() if key != "review"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def result_template():
    return {"schema": SCHEMA, "decision": "NOT_ACCEPTED", "mechanical_checks": "failed", "semantic_coverage": "manual_review_required", "all_telegram_coverage": False, "reasons": [], "counts": {"raw_candidate_rows": 0, "raw_candidate_ids": 0, "registry_ids": 0, "accepted_ids": 0, "verified_evidence": 0, "unresolved_material_selected_frontier": 0}, "limitations": ["Offline verification checks supplied snapshots, not snapshot authenticity or unobserved activity.", "External review is an attributed attestation, not independent automated journalism-quality verification."]}


def verify(manifest, root):
    result = result_template()
    reasons = result["reasons"]
    counts = result["counts"]

    def fail(code, detail):
        reasons.append({"code": code, "detail": str(detail)})

    def attempt(code, action):
        try:
            return action()
        except (ValueError, TypeError, KeyError, IndexError, OSError, AttributeError, OverflowError) as error:
            fail(code, error)
            return None

    if not isinstance(manifest, dict) or manifest.get("schema") != SCHEMA:
        fail("manifest_invalid", "Expected external-research-acceptance/v1 object")
        return result
    allowed = {"schema", "identity", "launch", "run", "goal", "run_inventory", "empty_start", "report", "raw_candidates", "registry", "evidence", "frontier", "review"}
    if set(manifest) - allowed:
        fail("manifest_invalid", "Unknown fields: " + ", ".join(sorted(set(manifest) - allowed)))
    identity = manifest.get("identity")
    if not isinstance(identity, dict) or set(identity) != {"thread_id", "run_id"} or not all(text(x) for x in identity.values()):
        fail("identity_mismatch", "Exact thread_id and run_id strings required")
        return result
    result["identity"] = identity

    def check_identity(value):
        require(isinstance(value, dict) and all(value.get(k) == v for k, v in identity.items()), "Exact thread/run identity absent or different")

    documents = {}
    for key in ("launch", "run", "goal"):
        document = attempt(key + "_unproven", lambda key=key: load_ref(root, manifest.get(key)))
        if isinstance(document, dict):
            documents[key] = document
            attempt("identity_mismatch", lambda document=document: check_identity(document))
        elif document is not None:
            fail(key + "_unproven", "Object required")
    run = documents.get("run", {})
    goal = documents.get("goal", {})
    launch = documents.get("launch", {})
    result["run_status"] = run.get("status")
    result["goal_status"] = goal.get("status")
    if run.get("status") != "success":
        fail("run_not_success", "Observed status: " + str(run.get("status")))
    if goal.get("status") != "satisfied":
        fail("goal_not_satisfied", "Observed status: " + str(goal.get("status")))

    def check_runs():
        inventory = load_ref(root, manifest.get("run_inventory"))
        require(inventory["thread_id"] == identity["thread_id"], "inventory thread mismatch")
        runs = inventory["runs"]
        require(isinstance(runs, list), "run inventory must be a list")
        if len(runs) != 1 or runs[0].get("run_id") != identity["run_id"] or runs[0].get("thread_id") != identity["thread_id"]:
            fail("not_single_run", "Run inventory is not exactly the expected one run")
        if timestamp(inventory["observed_at"]) < timestamp(run["updated_at"]):
            fail("run_inventory_stale", "Inventory predates final run observation")
    attempt("single_run_unproven", check_runs)

    def check_empty():
        snapshot = load_ref(root, manifest.get("empty_start"))
        require(isinstance(snapshot, dict) and snapshot.get('thread_id') == identity['thread_id'], 'prelaunch thread identity absent or different')
        require('run_id' not in snapshot or snapshot['run_id'] == identity['run_id'], 'prelaunch run identity differs')
        require(timestamp(snapshot["observed_at"]) < timestamp(launch["created_at"]), "empty snapshot not before launch")
        require(all(snapshot.get(key) == [] for key in ("entries", "prior_run_ids", "prior_message_ids")), "nonempty or incomplete start inventory")
    attempt("empty_start_unproven", check_empty)

    def check_report():
        report = manifest.get("report")
        if not isinstance(report, dict) or report.get("kind") != "final":
            raise ValueError("No designated final report; progress is not final")
        content = load_ref(root, report["artifact"], raw=True)
        require(text(content), "Final report empty or not text")
        return content
    report_content = attempt("final_report_missing", check_report)

    def rows(key, code):
        value = manifest.get(key)
        if not isinstance(value, list):
            fail(code, key + " must be an explicit list")
            return []
        return value

    candidate_ids = set()
    candidates = rows("raw_candidates", "candidates_unproven")
    counts["raw_candidate_rows"] = len(candidates)
    for index, row in enumerate(candidates):
        def check_candidate():
            require(text(row["id"]), "candidate ID must be a nonempty exact string")
            source_id = load_ref(root, row["source"])
            require(type(source_id) in (str, int) and str(source_id) == row["id"], "candidate source ID mismatch")
            candidate_ids.add(row["id"])
        attempt("candidate_invalid", check_candidate)
    counts["raw_candidate_ids"] = len(candidate_ids)

    verified = {}
    message_keys = set()
    for row in rows("evidence", "evidence_invalid"):
        def check_evidence():
            require(text(row["id"]) and row["id"] not in verified, "duplicate or empty evidence ID")
            source = load_ref(root, row["source"])
            fields = row["fields"]
            require(set(fields) == {"group_id", "message_id", "date", "text"}, "explicit four source field pointers required")
            for key in ("group_id", "message_id"):
                actual = pointer(source, fields[key])
                require(text(row[key]) and type(actual) in (str, int) and str(actual) == row[key], key + " mismatch")
            require(row["date"] == pointer(source, fields["date"]), "date mismatch")
            timestamp(row["date"])
            quote = row["quote"]
            source_text = pointer(source, fields["text"])
            require(text(quote) and isinstance(source_text, str) and quote in source_text, "quote is not exact source substring")
            key = (row["group_id"], row["message_id"])
            require(key not in message_keys, "duplicate evidence source message")
            message_keys.add(key)
            verified[row["id"]] = row
        attempt("evidence_invalid", check_evidence)
    counts["verified_evidence"] = len(verified)
    if not verified:
        fail("evidence_missing", "No independently reread evidence rows")

    registry = {}
    accepted = set()
    for row in rows("registry", "registry_invalid"):
        def check_registry():
            node = row["id"]
            require(text(node) and node not in registry, "duplicate or empty registry ID")
            require(row["disposition"] in {"accepted", "rejected", "deferred", "blocked", "discovered"}, "unknown disposition")
            require(text(row["reason"]), "disposition rationale missing")
            registry[node] = row
            if row["disposition"] == "accepted":
                accepted.add(node)
                ids = row.get("evidence_ids")
                if not isinstance(ids, list) or not ids or any(not isinstance(eid, str) or eid not in verified or verified[eid]["group_id"] != node for eid in ids):
                    fail("accepted_without_evidence", node)
        attempt("registry_invalid", check_registry)
    counts["registry_ids"] = len(registry)
    counts["accepted_ids"] = len(accepted)
    if candidate_ids != set(registry):
        fail("candidate_registry_mismatch", {"raw_without_disposition": sorted(candidate_ids - set(registry)), "registry_without_raw": sorted(set(registry) - candidate_ids)})
    if not accepted:
        fail("accepted_nodes_missing", "No accepted evidence-backed nodes")

    frontier_ids = set()
    for row in rows("frontier", "frontier_unproven"):
        def check_frontier():
            require(text(row["id"]) and row["id"] not in frontier_ids, "duplicate or empty frontier ID")
            frontier_ids.add(row["id"])
            require(type(row["selected"]) is bool and type(row["material"]) is bool, "frontier selection/materiality must be explicit booleans")
            source = load_ref(root, row["source"])
            closed = row["status"] in {"resolved", "traversed"} and source["status"] == row["status"] and source.get("remaining_window") is None
            if row["selected"] and row["material"] and not closed:
                counts["unresolved_material_selected_frontier"] += 1
                fail("frontier_unresolved", row["id"] + ": " + str(source.get("status")))
            elif not closed and not text(row.get("rationale")):
                fail("frontier_unproven", row["id"] + ": exclusion rationale absent")
        attempt("frontier_unproven", check_frontier)

    result["mechanical_checks"] = "failed" if reasons else "passed"

    def check_review():
        review = load_ref(root, manifest["review"])
        check_identity(review)
        require(text(review.get("reviewer")) and text(review.get("scope")), "reviewer and scope required")
        require(timestamp(review["reviewed_at"]) >= timestamp(run["updated_at"]), "review predates outcome")
        require(review.get("decision") == "passed", "external review did not pass")
        require(review.get("manifest_sha256") == review_digest(manifest), "review bound to different manifest")
        require(review.get("report_sha256") == manifest["report"]["artifact"]["sha256"], "review bound to different report")
        criteria = review["criteria"]
        require(isinstance(criteria, list) and bool(criteria), "review criteria required")
        for criterion in criteria:
            require(criterion["status"] == "passed" and text(criterion["criterion"]) and text(criterion["rationale"]), "unresolved review criterion")
            require(text(criterion.get("report_quote")) and isinstance(report_content, str) and criterion["report_quote"] in report_content, "review passage absent from final report")
            ids = criterion["evidence_ids"]
            require(isinstance(ids, list) and bool(ids) and all(eid in verified for eid in ids), "review evidence missing")
        result["semantic_coverage"] = "externally_reviewed"
        result["external_reviewer"] = review["reviewer"]
    if manifest.get("review") is None:
        fail("manual_review_required", "Scope, source relevance, final-versus-progress substance, frontier selection and journalism quality require external review")
    else:
        attempt("review_invalid", check_review)
    if not reasons:
        result["decision"] = "ACCEPTED"
    return result


def verify_saved_run(root, directory):
    root = Path(root)
    refs = {}
    saved = {}
    for name in ("launch", "latest-run", "latest-goal", "latest-state", "verified-report", "evidence-audit", "artifact-inventory"):
        path = (PurePosixPath(directory) / (name + ".json")).as_posix()
        data = read_scoped(root, path)
        refs[name] = {"path": path, "sha256": hashlib.sha256(data).hexdigest()}
        saved[name] = decode(data)
    launch = saved["launch"]
    audit = saved["evidence-audit"]
    manifest = {"schema": SCHEMA, "identity": {key: launch[key] for key in ("thread_id", "run_id")}, "launch": refs["launch"], "run": refs["latest-run"], "goal": dict(refs["latest-goal"], pointer="/goal"), "run_inventory": None, "empty_start": None, "report": None, "raw_candidates": [], "registry": [], "evidence": [], "frontier": [], "review": None}
    for index, item in enumerate(audit.get("manifests", [])):
        manifest["frontier"].append({"id": item["path"], "selected": True, "material": True, "status": item["status"], "source": dict(refs["evidence-audit"], pointer="/manifests/" + str(index))})
    result = verify(manifest, root)
    result["adapter"] = "saved-run-summary/v1: conservative negative-only adapter; no summary booleans establish acceptance"
    result["input_files"] = refs
    result["reported_counts_unverified"] = {key: audit.get(key) for key in ("raw_candidate_ids", "registry_unique_ids", "evidence_rows")}
    result["reported_counts_unverified"]["accepted_registry_nodes"] = len(audit.get("accepted_registry_nodes", []))
    for name in ("latest-run", "verified-report"):
        if any(saved[name].get(key) != value for key, value in manifest["identity"].items()):
            result["reasons"].append({"code": "identity_mismatch", "detail": name})
    result["reasons"].append({"code": "provenance_unproven", "detail": "Saved summary lacks quote text/source hashes and complete candidate dispositions; pass a normalized manifest to reread raw artifacts"})
    raw_count = audit.get("raw_candidate_ids")
    registry_count = audit.get("registry_unique_ids")
    if isinstance(raw_count, int) and isinstance(registry_count, int) and raw_count != registry_count:
        result["reasons"].append({"code": "reported_candidate_registry_mismatch", "detail": {"raw_candidate_ids": raw_count, "registry_unique_ids": registry_count}})
    result["limitations"].append("Adapter treats all saved collector manifests as selected/material conservatively; it does not authenticate audit snapshots or recompute their reported counts.")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description="Offline, read-only acceptance of a single saved research run")
    parser.add_argument("--root", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--manifest")
    mode.add_argument("--saved-run")
    args = parser.parse_args(argv)
    try:
        if args.saved_run:
            result = verify_saved_run(args.root, args.saved_run)
        else:
            result = verify(decode(read_scoped(args.root, args.manifest)), args.root)
    except (ValueError, TypeError, KeyError, IndexError, OSError, AttributeError, OverflowError) as error:
        result = result_template()
        result["reasons"].append({"code": "input_invalid", "detail": str(error)})
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["decision"] == "ACCEPTED" else 1


if __name__ == "__main__":
    sys.exit(main())
