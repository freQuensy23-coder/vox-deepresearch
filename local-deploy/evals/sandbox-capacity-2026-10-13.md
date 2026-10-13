# Shared research sandbox: 13 October 2026

The production kernel recorded a cgroup OOM at 13:09:26 UTC. It killed
`python-server` inside the research sandbox under the configured 1 GiB limit.
The process snapshot contained 112 processes and approximately 1048 MiB summed
RSS (shared pages can be counted more than once). Gateway transport/DNS failures
followed the exit. This evidence does not attribute upstream Vox HTTP 524 errors
to the sandbox.

The API-only container inherited a browser-dependent image healthcheck despite
not starting the browser. It therefore reported `unhealthy` while its API worked.

The revised provisioner uses 4 GiB RAM with no additional swap, 2 CPUs, 512 tasks,
64 native bash sessions, on-demand notebooks without prewarming, and an HTTP
healthcheck against `/v1/sandbox`. Persistent shell capacity still follows the
configured worker count (33 for 32 subagents plus the lead).

Verification used a separate sandbox on the production host, with an empty
fixture workspace, no upstream credentials, and no Vox traffic. Thirty-two native
asynchronous commands each held a 24 MiB allocation while an independent command
read a file. All 32 completed with exit code zero. Measured cgroup memory during
load was 1,139,343,360 bytes and peak was 1,153,527,808 bytes, exceeding the old
1,073,741,824-byte cap. `memory.events` reported zero OOMs and zero OOM kills.
This checks the shared execution capacity; it is not a complete research E2E or a
guarantee for arbitrary agent commands.

The 25 provisioner tests passed locally. An initial host-published fixture address
was unusable under rootless Docker; the load test used the fixture's API directly
inside its container. An initial polling assertion incorrectly expected the first
output chunk to be terminal; the completed check collected incremental output
until each command finished. Neither failed test attempt was counted as a pass.
