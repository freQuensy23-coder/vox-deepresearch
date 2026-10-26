"""A skill's report form wins over the lead prompt's "Sources" rule.

The citations block asked for a "Sources" section at the end of every report, so
Telegram research reports ended with «Источники», which their form forbids.
"""

from deerflow.agents.lead_agent.prompt import SYSTEM_PROMPT_TEMPLATE


def test_every_sources_rule_yields_to_a_skill_defined_report():
    citations = SYSTEM_PROMPT_TEMPLATE.split("<citations>")[1].split("</citations>")[0]
    rules = [line for line in citations.splitlines() if '"Sources" section' in line]
    assert len(rules) == 3
    assert all("loaded skill defines" in line for line in rules)
