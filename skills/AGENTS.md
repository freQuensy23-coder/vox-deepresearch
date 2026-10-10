# Public skill development

- `skills/public/telegram-research/` is an agent-led public Telegram research
  guide, with a stdlib-only Vox CLI for individual read operations. Keep search
  strategy and link/participant expansion in the Markdown instructions, not an
  automated research loop. Follow observed leads; missing links or usable
  authors must not block completion. The agent should finish reading selected
  useful, accessible candidates before answering; a group card alone does not
  establish content relevance. Repeat keyword, community-reference, and topical
  key-participant expansion over the growing set of chats until the user's
  question can be answered with evidence. Distinguish influential, substantive
  participants from weak advertising leads. Declare `VOX_API_KEY` as a skill secret; Gateway env
  alone is scrubbed by sandbox policy. Text search uses date boundaries rather
  than its disabled offset/phrase/language filters. Preserve author IDs, source
  links, full text and partial-result metadata. Participant panels are discovery
  leads, not proof of current membership; verify public group cards before use.
  Content commands can reuse a saved Vox card through `--public-card`; validate
  group identity and public fields locally rather than forcing a duplicate
  network lookup for every read. Keep provenance in the agent's research log.
  Vox's user-username lookup does not resolve group aliases. Use observed numeric
  group IDs or a saved matching card; refresh and verify aliases before following
  a link. Do not invent a group-lookup endpoint or convert a user ID into a chat ID.
  Text search requests `stream=true`: consume NDJSON progress until the final
  SearchResult, preserve partial-result metadata, and fail if no final result
  arrives. Stop reading once the final result arrives. The documented server
  timeout range is 10–600 seconds; progress is not a successful empty search.
  `group-preview` uses POST /reporter/parse with processing explicitly disabled:
  run_parser=false, find_new_users=false, new_users_limit=0.
  HTTP reads retry one transient disconnect/502/503/504/524 with a visible
  diagnostic; `--attempts 1` disables that transport retry. Do not automatically
  retry auth errors, not-found responses, rate limits or research pagination.
  Offline transport contracts live in `tests/skills/test_telegram_research.py`;
  live behavioral checks and their limits are in the skill's `evals/` directory.
