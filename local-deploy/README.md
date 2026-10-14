# Native Vox deployment

`runpod_native.py` starts the proxy, Gateway, frontend, Vox relay, Jev bridge and
Jev gateway directly on the RunPod host. Research uses `LocalSandboxProvider`
with host Bash enabled; it does not create another container or VM.

## Configuration

Everything is set in the repository's `config.yaml`: agent limits, the
`vox_relay` and `jev_bridge` sections, and `sandbox.environment` (the relay and
bridge URLs the skill scripts use). The proxy config is `runpod-nginx.conf`.

Secrets are the only thing outside git, and all of them are in the single
repository-root `.env`: `VOX_API_KEY` (Vox relay), `JEV_GATEWAY_API_KEY` (Jev
bridge and gateway) and the Jev gateway's provider keys. The Codex login is the
service user's `~/.codex/auth.json`.

The lead and depth prompts tell agents to write only inside their assigned
virtual thread folder. Bash command text is not scanned for file paths. Virtual
paths are translated to the thread's actual workspace and structured file tools
retain their path mapping and ownership checks.

Background commands return after at most 10 seconds if still running. Their
handles remain available to the owning execution until its release; there is no
additional cap of 64 commands.

Vox uses one upstream request at a time behind a FIFO queue. Retries,
`Retry-After`, and stream fallbacks are handled inside the proxy. Its
`call_timeout` (50 s) applies to each individual HTTP call to Vox; queue and
retry waits are outside that clock. Fast failures retry without an overall
retry deadline. A call exceeding it advises reducing the date interval. See [the proxy contract](VOX_PROXY.md).

The Jev client and bridge forward optional batch policy unchanged. Omitting it
uses the gateway's own defaults; the research system adds no free-only policy,
batch concurrency setting, or question/criteria size restrictions.
