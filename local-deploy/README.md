# Host AIO provisioner

Python standard library only. Run one host-side process, never inside the gateway or sandbox. The API token authorizes management of all broker-owned sandboxes; the authenticated gateway remains responsible for user authorization. Owner/thread labels prevent rebinding an existing sandbox ID to a different identity.

Before deployment, the operator must:

- Pre-pull `enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:1.11.0` into rootless Docker at `unix:///run/user/1001/docker.sock`. Creation uses `--pull never`.
- Create the Docker network `vox-research-execution` and configure its network isolation/egress policy separately. Proxy environment variables are not an egress firewall.
- Provide `/home/private/vox-deepresearch/skills` and the restricted VOX key file `/home/private/vox-deepresearch/.deployment/vox-api-key`.
- Provide a separate, nonempty provisioner token file readable only by the host operator. Pass the same token to DeerFlow as its provisioner API key. Never mount this token into a sandbox.
- Keep mount-source ancestors host-controlled, without symlinks. The broker rejects all symlink redirection, including within the project. Validate AIO process permissions on the scoped user-data directory and VOX key during deployment.

Start command for the deploying operator:

```sh
VOX_PROVISIONER_TOKEN_FILE=/absolute/path/to/provisioner-token python3 -B /home/private/vox-deepresearch/local-deploy/aio_provisioner.py
```

Configuration:

| Variable | Default |
| --- | --- |
| `VOX_PROVISIONER_HOST` | `127.0.0.1` |
| `VOX_PROVISIONER_PORT` | `18092` |
| `VOX_PROVISIONER_TOKEN_FILE` | Required |
| `VOX_SANDBOX_HTTP_PROXY` | Unset; fixed HTTP/HTTPS proxy environment when supplied |

Keep the listener on loopback unless the parent transport provides equivalent access control. Returned AIO URLs are always host loopback, `http://127.0.0.1:<random-port>`; gateway access/transport is outside this broker.

Authenticated endpoints: `GET /api/sandboxes`, `GET /api/sandboxes/{id}`, `POST /api/sandboxes`, `DELETE /api/sandboxes/{id}`. Every request requires `X-API-Key`. Create requires `sandbox_id`, `user_id`, and `thread_id`, each matching `[a-zA-Z0-9_-]{1,64}`. Only the existing remote backend's nonprivileged metadata/options are accepted. Extra mounts, custom skills roots, integration provisioning, and runtime policy overrides are rejected.

Quota is three owned containers, including stopped containers. Docker labels are the lifecycle registry; process restarts recover existing sandboxes. Same-scope creates are idempotent while running. Stopped sandboxes require explicit deletion before recreation. A timed-out or failed Docker run may leave a container, which remains discoverable and counts toward quota; retry discovery or delete it rather than assuming rollback. Running status reflects Docker state, not AIO application readiness.

Tests do not start a service or invoke real Docker:

```sh
python3 -B -m unittest discover -s local-deploy -p 'test_*.py' -v
```

The provisioner accepts `max_shell_sessions` from 1 through 65 (the harness
ceiling of 64 subagents plus the lead), forwards it to the sandbox environment,
and reports the configured container value. The default remains 10; a Gateway
with 32 execution slots requests 33 sessions. This does not change sandbox
count, mount policy, or resource limits.

## Shared sandbox resources

One research thread shares one sandbox across the lead and its depth agents;
32 agents do not create 32 containers. The provisioner now gives that container
4 GiB RAM (no additional swap), 2 CPUs, and 512 tasks. Native bash sessions
allow 64 managed commands, matching the harness registry. The requested
persistent-shell capacity remains lead + execution slots (33 for 32 workers).
Notebook prewarming is disabled with `JUPYTER_POOL_SIZE=0`; notebooks remain
available on demand.

The API-only entrypoint overrides the image's browser-dependent healthcheck
with an HTTP check of `/v1/sandbox` on port 8080 (3-second request timeout,
10-second interval, 30-second startup grace). A green healthcheck confirms API
availability, not completion of a research task. Container count remains capped
at three; memory limits are ceilings, not reservations or a guarantee for
arbitrary workloads.

These changes address the 13 October 2026 incident: the kernel killed the shared
`python-server` at 13:09:26 UTC under the previous 1 GiB cgroup limit. Transport
and DNS errors followed the container exit. This is separate from upstream Vox
HTTP 524 responses. Existing containers need an explicit live resource update;
new health/session/pool settings take effect on creation. Do not destroy an
active research sandbox merely to refresh its healthcheck.
