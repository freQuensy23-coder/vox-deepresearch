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
additional cap of 64 commands. Research supports 32 concurrent depth agents,
1000 total agents and a queue of 1000, without an overall subagent time limit.

Vox's adaptive relay is described in [VOX_DYNAMIC_ADMISSION.md](VOX_DYNAMIC_ADMISSION.md).
