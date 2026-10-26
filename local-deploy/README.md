# Native Vox deployment

Deploy only with `scripts/deploy-prod.sh` from the pushed `main`; never change the
prod host by hand. The script finds the pod through the RunPod API (its SSH port
changes on restart), ships the root `.env`, checks out the same commit on the
pod, installs Node into `.node` and Python deps with `uv sync`, builds the
frontend (Next standalone), restarts every service with `runpod_native.py`
(proxy, Gateway, frontend, Vox relay, t.me relay, Jev bridge and Jev gateway)
and checks their health. Research uses `LocalSandboxProvider` with host Bash enabled; it
does not create another container or VM.

## Configuration

Everything is set in the repository's `config.yaml`: agent limits, the
`vox_relay`, `tme_relay` and `jev_bridge` sections, and `sandbox.environment`
(the relay and bridge URLs the skill scripts use). The proxy config is `runpod-nginx.conf`.

Secrets are the only thing outside git, and all of them are in the single
repository-root `.env`: `VOX_API_KEY` (Vox relay), `JEV_GATEWAY_API_KEY` (Jev
bridge and gateway) and `AZURE_DECISION_API_KEY` (the Microsoft Foundry key the
Jev gateway uses for its Microsoft-Decision-1 deployment). The Codex login is the
service user's `~/.codex/auth.json`.

The UI has no login (`DEER_FLOW_AUTH_DISABLED=1` for Gateway and frontend): the
proxy listens on the pod's loopback only and is opened through an SSH tunnel
(`ssh -L 8800:127.0.0.1:8000 ...`, then http://localhost:8800). Anonymous
requests act as the existing account (`DEER_FLOW_AUTH_DISABLED_USER_ID` in
`runpod_native.py`), so its threads and `research.py` runs stay listed.

The lead and depth prompts tell agents to write only inside their assigned
virtual thread folder. Bash command text is not scanned for file paths. Virtual
paths are translated to the thread's actual workspace and structured file tools
retain their path mapping and ownership checks.

Background commands return after at most 10 seconds if still running. Their
handles remain available to the owning execution until its release; there is no
additional cap of 64 commands.

Vox uses one upstream request at a time behind a FIFO queue. Retries,
`Retry-After`, and stream fallbacks are handled inside the proxy. Its
`call_timeout` (250 s) applies to each individual HTTP call to Vox; queue and
retry waits are outside that clock. Fast failures retry without an overall
retry deadline. A call exceeding it means the query is too heavy. See [the proxy contract](VOX_PROXY.md).

The Jev client and bridge forward optional batch policy unchanged. Omitting it
uses the gateway's own defaults; the research system adds no free-only policy,
batch concurrency setting, or question/criteria size restrictions.

The t.me relay (`tme_relay.py`) serves the deep stage: once the first report is
ready, `deep_stage` in `config.yaml` sends the thread «Теперь сделай более
глубокое исследование», and `tme.py` opens public t.me pages through the relay.
It forwards only public chat and channel pages (a channel's post listing, one
message, one comment, the latest comments of a post), never `t.me/c/…` or invite
links, and sends no cookies or Telegram session. Every research thread on the
host shares its pace: one request at a time, `interval_seconds` apart. A refusal
from t.me (429, 403, 5xx, network error) pauses all requests (Retry-After or a
doubling backoff up to `backoff_max_seconds`) and retries; after
`give_up_seconds` of refusals the request fails and the agent is told to do
other work.
