# Public skill development

- `skills/public/telegram-research/` is an agent-driven Telegram research pipeline
  (see its SKILL.md): keyword search into one deduplicated message pool, Jev
  filtering with a one-pass review and re-runs, a keywords subagent and a whales
  subagent, whale messages from public chats added to the pool, repeated rounds.
  `scripts/vox.py` (fire) is the only Vox client and calls only the Vox proxy,
  which allows only text search and the user_tracker messages panel;
  `scripts/jev.py` talks only to the Jev bridge. Scripts fetch, merge and count;
  the agent chooses keywords, the Jev instruction and when to stop.
  Offline contracts live in `tests/skills/test_telegram_research.py`.
