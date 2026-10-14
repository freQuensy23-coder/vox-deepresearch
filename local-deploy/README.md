# Native Vox deployment

`runpod_native.py` starts Gateway, frontend, Vox relay and Jev directly on the
RunPod host. Research uses `LocalSandboxProvider` with host Bash enabled; it does
not create another container or VM. Existing scheduler and channel configuration
is preserved.

The lead and depth prompts tell agents to write only inside their assigned
virtual thread folder. Bash command text is not scanned for file paths. Virtual
paths are translated to the thread's actual workspace and structured file tools
retain their path mapping and ownership checks.

Background commands return after at most 10 seconds if still running. Their
handles remain available to the owning execution until its release; there is no
additional cap of 64 commands.

The gateway reads the deployment's `config.yaml` as is; all limits (subagent
concurrency, queue, timeouts, bash deadline, summarization) are set there and
nowhere else.

Vox's adaptive relay is described in [VOX_DYNAMIC_ADMISSION.md](VOX_DYNAMIC_ADMISSION.md).

The Jev client and bridge forward optional batch policy unchanged. Omitting it
uses the gateway's own defaults; the research system adds no free-only policy,
batch concurrency setting, or question/criteria size restrictions.
