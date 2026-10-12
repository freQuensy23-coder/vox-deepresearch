import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


MODULE_PATH = Path(__file__).with_name("research_acceptance.py")


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(MODULE_PATH.is_file(), "offline acceptance verifier is missing")
        spec = importlib.util.spec_from_file_location("research_acceptance", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        self.api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.api)
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.identity = {"thread_id": "thread-synthetic", "run_id": "run-synthetic"}
        self.manifest = {
            "schema": "external-research-acceptance/v1",
            "identity": self.identity,
            "launch": self.save("launch.json", dict(self.identity, created_at="2026-01-02T00:00:00Z")),
            "run": self.save("run.json", dict(self.identity, status="success", updated_at="2026-01-03T00:00:00Z")),
            "goal": self.save("goal.json", dict(self.identity, status="satisfied")),
            "run_inventory": self.save("runs.json", {"thread_id": "thread-synthetic", "observed_at": "2026-01-03T00:01:00Z", "runs": [self.identity]}),
            "empty_start": self.save("empty.json", dict(self.identity, observed_at="2026-01-01T23:59:59Z", entries=[], prior_run_ids=[], prior_message_ids=[])),
            "report": {"kind": "final", "artifact": self.save("report.txt", "Synthetic final report: a test quotation.")},
            "raw_candidates": [],
            "registry": [{"id": "-1001", "disposition": "accepted", "reason": "Synthetic evidence", "evidence_ids": ["e1"]}],
            "evidence": [],
            "frontier": [],
            "review": None,
        }
        raw = {"group_id": "-1001", "message_id": "7", "date": "2026-01-02T12:00:00Z", "text": "Here is a test quotation."}
        ref = self.save("raw.json", raw)
        self.manifest["raw_candidates"] = [{"id": "-1001", "source": dict(ref, pointer="/group_id")}]
        self.manifest["evidence"] = [{"id": "e1", "group_id": "-1001", "message_id": "7", "date": raw["date"], "quote": "a test quotation", "source": ref, "fields": {"group_id": "/group_id", "message_id": "/message_id", "date": "/date", "text": "/text"}}]

    def save(self, path, value):
        data = value.encode() if isinstance(value, str) else json.dumps(value).encode()
        (self.root / path).write_bytes(data)
        return {"path": path, "sha256": hashlib.sha256(data).hexdigest()}

    def verify(self):
        return self.api.verify(self.manifest, self.root)

    def codes(self):
        return {x["code"] for x in self.verify()["reasons"]}

    def add_review(self):
        review = dict(self.identity, reviewer="synthetic-reviewer", reviewed_at="2026-01-04T00:00:00Z", decision="passed", manifest_sha256=self.api.review_digest(self.manifest), report_sha256=self.manifest["report"]["artifact"]["sha256"], scope="synthetic selected scope; not all Telegram", criteria=[{"criterion": "Synthetic scope and final report reviewed", "status": "passed", "rationale": "Test-only review record", "report_quote": "Synthetic final report", "evidence_ids": ["e1"]}])
        self.manifest["review"] = self.save("review.json", review)

    def test_mechanical_success_requires_semantic_review(self):
        result = self.verify()
        self.assertEqual(result["decision"], "NOT_ACCEPTED")
        self.assertEqual(result["semantic_coverage"], "manual_review_required")
        self.assertEqual(result["counts"], {"raw_candidate_rows": 1, "raw_candidate_ids": 1, "registry_ids": 1, "accepted_ids": 1, "verified_evidence": 1, "unresolved_material_selected_frontier": 0})
        self.assertEqual(self.codes(), {"manual_review_required"})

    def test_explicit_external_review_is_distinguished(self):
        self.add_review()
        result = self.verify()
        self.assertEqual(result["decision"], "ACCEPTED")
        self.assertEqual(result["semantic_coverage"], "externally_reviewed")
        self.assertFalse(result["all_telegram_coverage"])

    def test_stale_review_is_rejected(self):
        self.add_review()
        self.manifest["registry"][0]["reason"] = "Changed selection"
        self.assertIn("review_invalid", self.codes())

    def test_nonterminal_or_error_run_and_active_goal(self):
        for status in ("running", "pending", "error", "cancelled"):
            with self.subTest(status=status):
                self.manifest["run"] = self.save("run.json", dict(self.identity, status=status, updated_at="2026-01-03T00:00:00Z"))
                self.manifest["goal"] = self.save("goal.json", dict(self.identity, status="active"))
                self.assertTrue({"run_not_success", "goal_not_satisfied"} <= self.codes())

    def test_identity_mismatch(self):
        self.manifest["run"] = self.save("run.json", dict(self.identity, run_id="different", status="success"))
        self.assertIn("identity_mismatch", self.codes())

    def test_missing_identity_is_not_inferred(self):
        self.manifest["goal"] = self.save("goal.json", {"status": "satisfied"})
        self.assertIn("identity_mismatch", self.codes())

    def test_multiple_runs_and_stale_inventory(self):
        self.manifest["run_inventory"] = self.save("runs.json", {"thread_id": "thread-synthetic", "observed_at": "2026-01-02T00:00:00Z", "runs": [self.identity, dict(self.identity, run_id="other")]})
        self.assertTrue({"not_single_run", "run_inventory_stale"} <= self.codes())

    def test_prelaunch_observation_does_not_require_future_run_id(self):
        self.manifest['empty_start'] = self.save('empty.json', {'thread_id': self.identity['thread_id'], 'observed_at': '2026-01-01T23:59:59Z', 'entries': [], 'prior_run_ids': [], 'prior_message_ids': []})
        self.assertEqual(self.codes(), {'manual_review_required'})

    def test_prelaunch_observation_rejects_different_thread(self):
        self.manifest['empty_start'] = self.save('empty.json', {'thread_id': 'another-thread', 'observed_at': '2026-01-01T23:59:59Z', 'entries': [], 'prior_run_ids': [], 'prior_message_ids': []})
        self.assertIn('empty_start_unproven', self.codes())

    def test_prelaunch_observation_rejects_conflicting_run_id(self):
        self.manifest['empty_start'] = self.save('empty.json', dict(self.identity, run_id='other-run', observed_at='2026-01-01T23:59:59Z', entries=[], prior_run_ids=[], prior_message_ids=[]))
        self.assertIn('empty_start_unproven', self.codes())

    def test_empty_start_boolean_is_not_proof(self):
        self.manifest["empty_start"] = True
        self.assertIn("empty_start_unproven", self.codes())

    def test_nonempty_or_late_start_snapshot(self):
        self.manifest["empty_start"] = self.save("empty.json", dict(self.identity, observed_at="2026-01-03T00:00:00Z", entries=["prior"], prior_run_ids=[], prior_message_ids=[]))
        self.assertIn("empty_start_unproven", self.codes())

    def test_progress_and_empty_reports_fail(self):
        self.manifest["report"]["kind"] = "progress"
        self.assertIn("final_report_missing", self.codes())
        self.manifest["report"]["kind"] = "final"
        self.manifest["report"]["artifact"] = self.save("report.txt", " \n")
        self.assertIn("final_report_missing", self.codes())

    def test_hash_quote_date_and_identity_checked(self):
        for field, value in (("quote", "invented"), ("date", "2020-01-01"), ("group_id", "wrong"), ("message_id", "9")):
            original = copy.deepcopy(self.manifest["evidence"])
            with self.subTest(field=field):
                self.manifest["evidence"][0][field] = value
                self.assertIn("evidence_invalid", self.codes())
            self.manifest["evidence"] = original
        (self.root / "raw.json").write_text("{}")
        self.assertIn("evidence_invalid", self.codes())

    def test_paths_cannot_escape_scope(self):
        for path in ("../outside", "/etc/passwd", "https://example.com/raw.json"):
            with self.subTest(path=path):
                self.manifest["evidence"][0]["source"]["path"] = path
                self.assertIn("evidence_invalid", self.codes())
        (self.root / "link").symlink_to("/etc/passwd")
        self.manifest["evidence"][0]["source"]["path"] = "link"
        self.assertIn("evidence_invalid", self.codes())

    def test_candidate_dispositions_are_reconciled(self):
        self.manifest["raw_candidates"].append({"id": "-1002", "source": self.save("candidate.json", "\"-1002\"")})
        self.assertIn("candidate_registry_mismatch", self.codes())
        self.assertEqual(self.verify()["counts"]["accepted_ids"], 1)

    def test_duplicate_registry_and_evidence_fail(self):
        self.manifest["registry"].append(copy.deepcopy(self.manifest["registry"][0]))
        self.manifest["evidence"].append(copy.deepcopy(self.manifest["evidence"][0]))
        self.assertTrue({"registry_invalid", "evidence_invalid"} <= self.codes())

    def test_accepted_node_requires_verified_source(self):
        self.manifest["registry"][0]["evidence_ids"] = ["absent"]
        self.assertIn("accepted_without_evidence", self.codes())

    def test_selected_material_frontier_cannot_be_dismissed(self):
        self.manifest["frontier"] = [{"id": "f1", "selected": True, "material": True, "status": "resolved", "source": self.save("frontier.json", {"status": "paused", "remaining_window": {"to": "2025-01-01"}})}]
        self.assertIn("frontier_unresolved", self.codes())
        self.assertEqual(self.verify()["counts"]["unresolved_material_selected_frontier"], 1)

    def test_missing_frontier_ledger_does_not_mean_empty(self):
        del self.manifest["frontier"]
        self.assertIn("frontier_unproven", self.codes())

    def test_malformed_manifest_fails_closed(self):
        for manifest in ({}, [], {"schema": "wrong"}, dict(self.manifest, evidence="true")):
            with self.subTest(manifest=str(manifest)[:40]):
                self.assertEqual(self.api.verify(manifest, self.root)["decision"], "NOT_ACCEPTED")

    def test_mechanical_result_is_separate(self):
        self.assertEqual(self.verify().get("mechanical_checks"), "passed")
        self.manifest["report"] = None
        self.assertEqual(self.verify().get("mechanical_checks"), "failed")

    def test_review_passage_must_exist_in_actual_report(self):
        self.add_review()
        review = json.loads((self.root / "review.json").read_text())
        review["criteria"][0]["report_quote"] = "Invented report passage"
        self.manifest["review"] = self.save("review.json", review)
        self.assertIn("review_invalid", self.codes())

    def test_duplicate_json_keys_fail_closed(self):
        self.manifest["goal"] = self.save("goal.json", '{"status":"active","status":"satisfied"}')
        self.assertIn("goal_unproven", self.codes())

    def test_cli_json_and_no_writes(self):
        self.save("manifest.json", self.manifest)
        before = {p.name: p.read_bytes() for p in self.root.iterdir()}
        run = subprocess.run([sys.executable, "-B", str(MODULE_PATH), "--root", str(self.root), "--manifest", "manifest.json"], capture_output=True, text=True)
        self.assertEqual(run.returncode, 1, run.stderr)
        self.assertEqual(json.loads(run.stdout)["decision"], "NOT_ACCEPTED")
        self.assertEqual(before, {p.name: p.read_bytes() for p in self.root.iterdir()})

    def test_legacy_adapter_never_promotes_summary_flags(self):
        self.save("latest-run.json", dict(self.identity, status="error"))
        self.save("latest-goal.json", {"goal": {"status": "active"}})
        self.save("latest-state.json", {"values": {"messages": []}})
        self.save("verified-report.json", dict(self.identity, research_acceptance=True))
        self.save("evidence-audit.json", {"raw_candidate_ids": 755, "registry_unique_ids": 289, "accepted_registry_nodes": [{"id": 1}], "evidence_all_exact": True, "manifests": [{"path": "m", "status": "paused", "remaining_window": {"to": "2025"}}]})
        self.save("artifact-inventory.json", [])
        result = self.api.verify_saved_run(self.root, ".")
        self.assertEqual(result["decision"], "NOT_ACCEPTED")
        self.assertTrue({"run_not_success", "goal_not_satisfied", "empty_start_unproven", "single_run_unproven", "final_report_missing", "frontier_unresolved"} <= {r["code"] for r in result["reasons"]})
        self.assertEqual(result["reported_counts_unverified"]["raw_candidate_ids"], 755)


if __name__ == "__main__":
    unittest.main()
