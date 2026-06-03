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
