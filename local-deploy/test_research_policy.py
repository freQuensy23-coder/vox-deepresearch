import unittest
from pathlib import Path

import yaml


class ResearchPolicyTest(unittest.TestCase):
    def setUp(self):
        self.config = yaml.safe_load((Path(__file__).resolve().parents[1] / 'config.yaml').read_text())

    def test_shell_frequency_does_not_end_first_discovery_batch(self):
        loop = self.config['loop_detection']
        shell = loop.get('tool_freq_overrides', {}).get('bash', {})
        self.assertGreaterEqual(shell.get('warn', 0), 80)
        self.assertGreaterEqual(shell.get('hard_limit', 0), 120)
        self.assertTrue(loop['enabled'])
        self.assertEqual(loop['hard_limit'], 5)
        self.assertEqual(loop['tool_freq_hard_limit'], 50)

    def test_lead_distinguishes_concurrency_from_run_allowance(self):
        overlay = self.config.get('lead_prompt_overlay', {}).get('append', '')
        self.assertIn('max_concurrent_subagents', overlay)
        self.assertIn('max_total_subagents', overlay)
        self.assertIn('remaining', overlay)

    def test_child_handoffs_progress_before_limits(self):
        overlay = self.config['subagents'].get('agents', {}).get('general-purpose', {}).get('prompt_overlay', {}).get('append', '')
        self.assertIn('checkpoint', overlay)
        self.assertIn('unfinished', overlay)
        self.assertIn('frequency warning', overlay)


if __name__ == '__main__':
    unittest.main()
