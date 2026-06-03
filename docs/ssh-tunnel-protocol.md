# On-demand SSH tunnel protocol

This document is the **shared contract** between the two repositories that implement the
on-demand SSH tunnel:

- **`expidite`** — runs on the Raspberry Pi (the *device* side).
- **`expidite-portal`** — the Azure App Service web server (the *service* side).

## Why this exists

Pis connect **outbound** to Azure IoT Hub. They sit behind NAT and are **not** reachable from
the public internet — no inbound port is ever opened on the Pi. An operator nevertheless needs
to open an interactive SSH session to a chosen Pi from the portal.

The design reuses the existing IoT Hub direct-method channel **only to trigger** the tunnel.
The SSH bytes themselves travel inside a **WebSocket that the Pi opens outbound** to the portal.

**Key invariant:** the Pi only ever makes *outbound* connections. The portal never connects to
the Pi.

## Flow

1. Operator clicks "SSH" for a device in the portal. The request lands on App Service
   instance **X**.
2. Instance X mints a session: a `sessionId` plus a single-use `token` with a short TTL, reads
   its own `WEBSITE_INSTANCE_ID`, and stores the pending session **in memory** (keyed by
   `sessionId`).
3. Instance X invokes the IoT Hub direct method **`open_ssh_tunnel`** on the device, then acks
   fast.
4. The Pi handles `open_ssh_tunnel`, returns an immediate ack, then **asynchronously** opens a
   `wss://` connection to the portal, presenting the token + sessionId in headers and setting the
   ARR affinity cookie so the front end routes it back to instance X.
5. Instance X validates the token/sessionId against its pending session (rejecting with a
   distinct close code if unknown — e.g. the instance was recycled), then bridges the WebSocket
   to an SSH client. The Pi bridges its end of the WebSocket to `localhost:22` (sshd).
6. SSH runs end-to-end inside the WebSocket. On session end, timeout, or disconnect, both sides
   tear down.

## Direct method

- **Name:** `open_ssh_tunnel`

  (snake_case, matching the existing methods in both repos: `reboot`, `update_software`,
  `enter_review_mode`, `exit_review_mode`, `get_review_mode`.)

### Payload (portal → Pi)

```jsonc
{
  "sessionId": "5f1b...",            // uuid4 string
  "token": "<opaque secret>",        // crypto-random, single-use, short TTL
  "wssUrl": "wss://portal.example.net/internal/ssh-tunnel",
  "affinity": {                      // ARR affinity cookie to pin to the minting instance
    "name": "ARRAffinitySameSite",   // or "ARRAffinity"; derived from the portal's config
    "value": "<WEBSITE_INSTANCE_ID>"
  },
  "targetPort": 22,                  // optional, default 22 — local sshd port on the Pi
  "expiresAt": "2026-06-03T12:34:56Z"  // ISO-8601 UTC; the session is invalid after this
}
```

### Direct-method response (Pi → portal)

Returned **promptly**, well under the IoT Hub direct-method timeout. The actual dial-out happens
asynchronously *after* this response is sent.

```jsonc
{
  "accepted": true,
  "reason": "..."   // optional; present (and accepted=false) when the Pi declines
}
```

Reasons the Pi may decline (`accepted: false`): malformed payload, the session is already expired
(`expiresAt` in the past), or the device is at its concurrent-session cap.

## WebSocket connect (Pi → portal)

- **URL:** the `wssUrl` from the payload.
- **Headers:**
  - `X-Tunnel-Session: <sessionId>`
  - `X-Tunnel-Token: <token>`
  - `Cookie: <affinity.name>=<affinity.value>`
- **Secrets go in headers, never in the query string** (query strings get logged by proxies and
  access logs).

The portal validates `X-Tunnel-Session` + `X-Tunnel-Token` against the pending session
(constant-time compare), consumes the token (single use), and checks expiry. On success it
bridges the socket to an SSH client; on failure it closes with one of the codes below.

## Keepalive

The Pi sends a WebSocket **ping roughly every 30 seconds**. Azure App Service drops idle
connections at ~230 seconds, so the keepalive must stay well under that.

## Server close codes

The portal closes the WebSocket with an explicit code so the Pi (and operator) can react:

| Code   | Name             | Meaning                                                        |
|--------|------------------|---------------------------------------------------------------|
| `1000` | `normal-close`   | Session ended cleanly.                                         |
| `4401` | `auth-failed`    | Header/credential malformed or missing; generic auth failure. |
| `4403` | `expired-token`  | Token (or session) past `expiresAt`.                          |
| `4404` | `unknown-session`| No matching pending session — e.g. the minting instance was recycled, or ARR affinity routed the dial-back elsewhere. The portal should re-trigger from a live instance on the next operator action. |

Codes in the `4xxx` range are application-defined (the WebSocket spec reserves `4000`–`4999`
for private use).

## Security notes

- The `token` is never logged on either side. Structured logs are keyed by `sessionId` only.
- The operator supplies the SSH **password** for each connection. The portal holds it only in the
  in-memory pending session for the (short) session TTL, then uses it to authenticate to the Pi's
  sshd. The password is **never** persisted server-side, **never** logged, and **never** sent over
  IoT Hub (it stays portal-side — the device only bridges raw bytes to its local sshd). The SSH
  username is configured on the portal (`SSH_TUNNEL_SSH_USER`); a matching account must already
  exist on the Pis (provisioning may be out of scope of this change).
- The portal pins each device's SSH host key on first connect and refuses on later mismatch
  (per-device known-hosts). With password auth this matters especially: it stops a spoofed
  endpoint from harvesting the operator's password.
- The Pi dials out to whatever `wssUrl` the `open_ssh_tunnel` payload specifies, but **only over
  TLS** — a `wssUrl` that isn't `wss://` is rejected, so a payload cannot downgrade the device to a
  plaintext connection. There is no host allowlist on the device: invoking the direct method
  already requires privileged IoT Hub service access, and the device supports multiple portal
  instances without per-device config.

## Implementation note: portal SSH stack

The portal is synchronous Flask. The WebSocket endpoint uses **`flask-sock`** (runs on a
gunicorn `gthread` worker on App Service), and the SSH client is **`paramiko`** driven over a
socket-like adapter that wraps the WebSocket. This is a deliberate choice over `asyncssh`, which
would require an async event loop bridged into the synchronous request — more moving parts for no
benefit here.

## Interactive terminal (portal-internal)

This part does not affect the Pi↔portal wire contract above; it is how the operator drives the
shell from a browser.

When the device dials back, the portal authenticates, opens an interactive shell channel
(PTY), and registers it in memory keyed by `sessionId`, then keeps the tunnel open. The operator's
browser opens a **separate** WebSocket to the same instance (ARR affinity routes it there) and
attaches to that shell:

- The initiation response returns a one-time **browser token** (distinct from the device `token`)
  and the terminal page URL.
- The browser presents the browser token as the **first message** on its terminal WebSocket
  (never in a URL); the portal validates it (constant-time) and allows a single attach.
- Terminal input is sent as binary frames; control messages (`auth`, `resize`) are JSON text.
- The session stays open until the operator closes the terminal, the shell exits, or a configurable
  hard cap (`SSH_TUNNEL_MAX_SESSION_SECONDS`, default 8h) is reached. If no browser attaches within
  `SSH_TUNNEL_BROWSER_ATTACH_SECONDS` (default 60s), the tunnel is torn down.
- Server-side WebSocket keepalive pings (~25s) keep both connections under App Service's idle cut.

## Deployment (portal App Service)

The interactive terminal has a hard requirement: the device's tunnel WebSocket and the operator's
browser WebSocket must be handled **in the same Python process**, because they rendezvous through an
in-memory registry (`ACTIVE_TERMINALS`) and the pending-session store (`SESSION_STORE`). ARR
affinity pins both to the same *instance (VM)*, but **not** to the same gunicorn *worker process*.

So the portal must run as a **single gunicorn worker** with a thread/async worker class that can
serve many concurrent connections in that one process. A startup command such as:

```
gunicorn --workers 1 --threads 50 --worker-class gthread --timeout 0 <wsgi_module>:app
```

- `--workers 1` — one process, so the in-memory registry/session store is shared by every
  connection on the instance. (Scale out across *instances*, not worker processes; ARR affinity +
  the affinity cookie keep each session's two WebSockets on one instance.)
- `--threads 50` (or more) — `flask-sock` runs on the `gthread` worker; each live WebSocket holds a
  thread for its lifetime, so size this for the max concurrent tunnels + terminals you expect. (A
  `gevent`/`eventlet` worker is an alternative that scales connections more cheaply.)
- `--timeout 0` — disables gunicorn's worker timeout so a long-lived WebSocket isn't killed.

App Service settings (Portal: **Configuration → General settings**; or `az` as shown):

- **WebSockets: On** (`az webapp config set --web-sockets-enabled true`). Required.
- **ARR affinity / Session affinity: On** for multi-instance plans
  (`az webapp update --client-affinity-enabled true`) — when scaled out, it keeps a session's two
  WebSockets on one instance. The affinity cookie name is `ARRAffinity` (or `ARRAffinitySameSite`);
  set `SSH_TUNNEL_AFFINITY_COOKIE_NAME` to match, and the device payload's `affinity.value` is
  derived from `WEBSITE_INSTANCE_ID`. **On a single-instance plan (e.g. Free tier) this is a no-op**
  for routing — the cross-*process* `--workers 1` requirement above is what matters there.
- **Always On: recommended but optional** — *not available on Free/Shared (F1/D1)*. The feature
  still works without it: a live terminal is an open WebSocket, which is continuous activity, so an
  in-progress session won't be idle-unloaded. The only cost is a cold start on the first request
  after the app has been idle.

Required environment variables (secrets via Key Vault references):

- `AZURE_IOT_HUB_CONNECTION_STRING` — service-side IoT Hub access to invoke the direct method.
- `SSH_TUNNEL_SSH_USER` — SSH account on the Pis (default `bee-ops`). The password is supplied by
  the operator per connection, never stored.
- `WEBSITE_HOSTNAME` — provided automatically by App Service; used to build `wssUrl`.
- `SSH_TUNNEL_KNOWN_HOSTS_DIR` — set to a **persistent** path, e.g. `/home/ssh_known_hosts`. The
  default is relative/ephemeral, so on a tier without Always On the pinned host keys would be lost
  on each unload and the portal would silently re-trust (TOFU) the device on the next connect.
- Optional tuning: `SSH_TUNNEL_AFFINITY_COOKIE_NAME`, `SSH_TUNNEL_TTL_SECONDS`,
  `SSH_TUNNEL_TARGET_PORT`, `SSH_TUNNEL_BROWSER_ATTACH_SECONDS`, `SSH_TUNNEL_MAX_SESSION_SECONDS`.

> If the tunnel authenticates but the browser terminal shows no prompt, the usual cause is multiple
> worker processes: the browser WebSocket landed on a different process than the device's tunnel and
> can't find the registered shell. Confirm `--workers 1`.

### Worker count trade-off

`--workers 1 --threads N` keeps concurrency (the portal is mostly I/O-bound, which threads serve
well) while guaranteeing the two WebSockets share one process. On a single-core plan this is
effectively free and uses *less* memory than several worker processes (which each load
pandas/numpy/polars). The case where multiple workers genuinely help is a **multi-core paid plan
with CPU-heavy concurrent requests** (Python's GIL means CPU-bound work doesn't parallelise across
threads) — revisit then. The alternative that removes the single-worker constraint entirely is to
move `SESSION_STORE`/`ACTIVE_TERMINALS` into an external shared store (e.g. Redis); that is extra
complexity and only worth it on a scaled-out deployment.
