import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name('research_state_snapshot.py')


class SyntheticSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.registry = self.put('registry.json', {'candidates': []})

    def put(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding='utf-8')
        return path

    def run_snapshot(self, registry='registry.json', output='snapshot.json'):
        result = subprocess.run([sys.executable, str(SCRIPT), '--root', str(self.root), '--registry', registry, '--output', str(self.root / output)], capture_output=True, text=True)
        if result.returncode:
            self.assertIn('snapshot error:', result.stderr)
        return result

    def snapshot(self):
        result = self.run_snapshot()
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads((self.root / 'snapshot.json').read_text())

    def test_synthetic_counts_are_distinct_and_claimed_not_verified(self):
        self.put('registry.json', {'candidate_count': 999, 'accepted_count': 888, 'candidates': [
            {'id': -101, 'alias': 'synthetic_a', 'decision': 'accepted'},
            {'id': -101, 'decision': 'discovered'},
            {'id': None, 'alias': 'synthetic_a'},
            {'alias': 'synthetic_b'}, {'id': None, 'alias': 'synthetic_b'}]})
        before = self.registry.read_bytes()
        data = self.snapshot()
        self.assertEqual(data['registry']['numeric_ids'], [-101])
        self.assertEqual(data['registry']['unresolved_aliases'], ['synthetic_a', 'synthetic_b'])
        self.assertEqual(data['registry']['candidate_rows'], 5)
        self.assertEqual(data['registry']['accepted_claimed_numeric_ids'], [-101])
        self.assertEqual(data['registry']['evidence_verification'], 'not_performed')
        self.assertEqual(data['registry']['sha256'], hashlib.sha256(before).hexdigest())
        self.assertTrue(data['registry']['observed_at'])
        self.assertEqual(before, self.registry.read_bytes())
        self.assertNotIn('complete', data)

    def test_synthetic_numeric_identity_allows_empty_optional_alias(self):
        self.put('registry.json', {'candidates': [{'id': -123, 'alias': ''}]})
        self.assertEqual(self.snapshot()['registry']['numeric_ids'], [-123])

    def test_synthetic_malformed_rows_fail_closed(self):
        for row in [None, [], {}, {'id': None}, {'id': True, 'alias': 'synthetic'}, {'id': False}, {'id': '123'}, {'id': 1.0}, {'id': 0}, {'id': 1, 'alias': []}, {'id': 1, 'decision': []}]:
            with self.subTest(row=row):
                self.put('registry.json', {'candidates': [row]})
                result = self.run_snapshot()
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / 'snapshot.json').exists())

    def test_synthetic_traversed_manifest_preserves_null_window(self):
        self.put('done/manifest.json', {'status': 'traversed', 'parameters': {'query': 'synthetic'}, 'remaining_window': None})
        self.assertIsNone(self.snapshot()['collectors'][0]['remaining_window'])

    def test_synthetic_parent_child_remain_independent(self):
        for name, status, end in [('parent', 'paused', '2026-12-01'), ('child', 'traversed', '2026-02-01')]:
            self.put(f'{name}/manifest.json', {'status': status, 'parameters': {'query': 'synthetic', 'order': 'desc', 'from_date': '2026-01-01', 'to_date': end}, 'remaining_window': {'from': '2026-01-01', 'to': end}, 'supersedes': 'parent', 'traversal_complete': status == 'traversed'})
        self.put('unrelated.json', {'status': 'traversed'})
        data = self.snapshot()
        self.assertEqual(len(data['collectors']), 2)
        self.assertEqual([x['status'] for x in data['collectors']], ['traversed', 'paused'])
        for item in data['collectors']:
            self.assertEqual(item['relationship'], 'independent_observation')
            self.assertTrue(item['source_id'])
            self.assertTrue(item['sha256'])
            self.assertTrue(item['remaining_window'])
            self.assertTrue(item['parameters'])
            self.assertNotIn('traversal_complete', item)

    def test_synthetic_projection_hash_and_path_mismatches(self):
        self.put('registry.json', {'candidates': [], 'projections': ['old.json', 'other.json']})
        self.put('old.json', {'source_path': 'registry.json', 'source_sha256': '0' * 64, 'accepted_count': 999})
        self.put('other.json', {'source_path': 'other-registry.json'})
        data = self.snapshot()
        self.assertEqual(data['projections'][0]['issues'], ['source_hash_mismatch'])
        self.assertEqual(data['projections'][1]['issues'], ['source_path_mismatch'])
        self.assertEqual(data['registry']['accepted_claimed_numeric_ids'], [])

    def test_synthetic_paths_cannot_escape(self):
        for path in ['../registry.json', str(self.registry)]:
            result = self.run_snapshot(registry=path)
            self.assertNotEqual(result.returncode, 0)
        (self.root / 'linked').symlink_to(self.root, target_is_directory=True)
        self.assertNotEqual(self.run_snapshot(registry='linked/registry.json').returncode, 0)
        self.assertNotEqual(self.run_snapshot(output='linked/snapshot.json').returncode, 0)
        self.assertNotEqual(self.run_snapshot(output='../escaped-snapshot.json').returncode, 0)
        self.assertFalse((self.root.parent / 'escaped-snapshot.json').exists())

    def test_synthetic_manifest_symlinks_are_not_followed(self):
        (self.root / 'loop').symlink_to(self.root, target_is_directory=True)
        (self.root / 'manifest.json').symlink_to(self.registry)
        data = self.snapshot()
        self.assertEqual(data['collectors'], [])
        self.assertEqual(data['skipped_symlinks'], ['loop', 'manifest.json'])

    def test_synthetic_existing_output_is_prohibited(self):
        before = self.registry.read_bytes()
        self.assertNotEqual(self.run_snapshot(output='registry.json').returncode, 0)
        self.assertEqual(self.registry.read_bytes(), before)

    def test_synthetic_malformed_manifest_is_not_silently_omitted(self):
        self.put('bad/manifest.json', {'status': 'paused'})
        self.assertNotEqual(self.run_snapshot().returncode, 0)
        self.assertFalse((self.root / 'snapshot.json').exists())

    def test_synthetic_duplicate_json_keys_fail_closed(self):
        self.registry.write_text('{"candidates": [], "candidates": []}')
        self.assertNotEqual(self.run_snapshot().returncode, 0)


if __name__ == '__main__':
    unittest.main()
