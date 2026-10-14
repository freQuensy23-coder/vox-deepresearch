# Native Vox deployment

`runpod_native.py` starts the proxy, Gateway, frontend, Vox relay, Jev bridge and
Jev gateway directly on the RunPod host. Research uses `LocalSandboxProvider`
with host Bash enabled; it does not create another container or VM.

## Configuration

Everything is set in the repository's `config.yaml`: agent limits, the
`vox_relay` and `jev_bridge` sections, and `sandbox.environment` (the relay and
bridge URLs the skill scripts use). The proxy config is `runpod-nginx.conf`.

Secrets are the only thing outside git. They live in `.deployment/` at the
repository root:

| File | Used by |
| --- | --- |
| `vox-api-key` | Vox relay |
| `jev-gateway-key` | Jev bridge |
| `codex-auth.json` | Gateway models (copied to `~/.codex/auth.json`) |
| `jev-gateway.env`, `opencode-go.env` | Jev gateway |

The lead and depth prompts tell agents to write only inside their assigned
virtual thread folder. Bash command text is not scanned for file paths. Virtual
paths are translated to the thread's actual workspace and structured file tools
retain their path mapping and ownership checks.

Background commands return after at most 10 seconds if still running. Their
handles remain available to the owning execution until its release; there is no
additional cap of 64 commands.

Vox's adaptive relay is described in [VOX_DYNAMIC_ADMISSION.md](VOX_DYNAMIC_ADMISSION.md).

The Jev client and bridge forward optional batch policy unchanged. Omitting it
uses the gateway's own defaults; the research system adds no free-only policy,
batch concurrency setting, or question/criteria size restrictions.
