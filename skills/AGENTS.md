# Public skill development

- `skills/public/telegram-research/` is an agent-driven Telegram research pipeline
  (see its SKILL.md): keyword message search over the requested period, semantic
  group search, Jev filtering with eye review and re-runs, top-16 group ranking,
  full download of those groups and one subagent per group, repeated while new
  keywords appear. `scripts/vox.py` (fire) is the only Vox client and calls only
  the Vox proxy; `scripts/jev.py` talks only to the Jev bridge. Scripts fetch and
  count; the agent chooses keywords, the Jev instruction and when to stop.
  Offline contracts live in `tests/skills/test_telegram_research.py`.
