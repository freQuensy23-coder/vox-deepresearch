# Public skill development

- `skills/public/telegram-research/` is an agent-led public Telegram research
  guide, with a stdlib-only Vox CLI for individual read operations. Keep search
  strategy and link/participant expansion in the Markdown instructions, not an
  automated research loop. Follow observed leads; missing links or usable
  authors must not block completion. The lead must assign every discovered relevant group to depth research and
  account for all search candidates before declaring the requested map complete; a group card alone does not
  establish content relevance. Repeat keyword, community-reference, and topical
  key-participant expansion over the growing set of chats until the complete map requested by the user is established, without
  silently discarding relevant groups or unresolved work. Distinguish influential, substantive
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

Telegram research now uses a breadth lead and one depth subagent per group
(only confirmed related-group clusters may share an assignment). Preserve the
four shared registry lists, atomic locked updates, provenance, claims and
one-page sourced facts. Batch manifests dispatch independent group tasks;
helpers must not choose keywords, relevance or research stopping conditions.
Depth agents may use Jev for triage and inspect other public chats of topical
participants to propose new groups; the next depth assignment stays with the lead.

The lead may automate breadth search, triage and batch submission with scripts
and Jev. Depth agents may export their assigned group and analyze it with these
tools. Batch examples must dispatch all unassigned thematic groups in the
registry, with 32 running slots, 1000 total assignments and queue capacity 1000.
Do not replace incomplete group research with a selected sample or prohibit
further network collection because earlier requests failed.
