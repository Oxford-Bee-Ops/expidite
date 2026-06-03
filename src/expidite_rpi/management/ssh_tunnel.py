"""On-demand outbound SSH tunnel (device side).

The portal triggers a tunnel via the ``open_ssh_tunnel`` IoT Hub direct method. The device then
dials *outbound* to the portal over a WebSocket (the device never accepts inbound connections),
and bridges that WebSocket to its local sshd. See ``docs/ssh-tunnel-protocol.md`` for the wire
contract.

The byte pump is deliberately decoupled from the real network libraries (the WebSocket connect
and the local-socket connect are injected) so the bridge logic can be unit-tested with mocks.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlparse

from expidite_rpi.core import configuration as root_cfg

if TYPE_CHECKING:
    from websockets.sync.client import ClientConnection

logger = root_cfg.setup_logger("expidite")

DEFAULT_TARGET_PORT = 22
# Ping well under App Service's ~230s idle timeout (see docs/ssh-tunnel-protocol.md).
PING_INTERVAL_SECONDS = 30
# How long the local sshd connect may take before we give up.
LOCAL_CONNECT_TIMEOUT_SECONDS = 10
# Chunk size for reads off the local socket.
_SOCKET_READ_SIZE = 65536


class WebSocketLike(Protocol):
    """The subset of the websockets sync ClientConnection that the bridge uses."""

    def send(self, message: bytes | str) -> None: ...

    def recv(self, timeout: float | None = None) -> bytes | str: ...

    def close(self, code: int = 1000, reason: str = "") -> None: ...


class SocketLike(Protocol):
    """The subset of socket.socket that the bridge uses."""

    def recv(self, bufsize: int, /) -> bytes: ...

    def sendall(self, data: bytes, /) -> None: ...

    def close(self) -> None: ...


# A WebSocket connect callable: (wss_url, headers) -> WebSocketLike
WsConnect = Callable[[str, dict[str, str]], WebSocketLike]
# A local-socket connect callable: (host, port) -> SocketLike
SockConnect = Callable[[str, int], SocketLike]


def _parse_expires_at(raw: str) -> datetime | None:
    """Parse an ISO-8601 UTC timestamp; return None if unparseable."""
    try:
        parsed = datetime.fromisoformat(raw)
    except (ValueError, AttributeError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _default_ws_connect(wss_url: str, headers: dict[str, str]) -> WebSocketLike:
    from websockets.sync.client import connect

    conn: ClientConnection = connect(
        wss_url,
        additional_headers=headers,
        ping_interval=PING_INTERVAL_SECONDS,
        open_timeout=LOCAL_CONNECT_TIMEOUT_SECONDS,
    )
    return conn


def _default_sock_connect(host: str, port: int) -> SocketLike:
    return socket.create_connection((host, port), timeout=LOCAL_CONNECT_TIMEOUT_SECONDS)


def _pump_ws_to_sock(
    ws: WebSocketLike, sock: SocketLike, stop_event: threading.Event, session_id: str
) -> None:
    """Forward bytes from the WebSocket to the local socket until either side closes."""
    try:
        while not stop_event.is_set():
            message = ws.recv()
            data = message.encode("utf-8") if isinstance(message, str) else message
            if not data:
                continue
            sock.sendall(data)
    except Exception:
        logger.info(f"SSH tunnel {session_id}: ws->sock pump ended")
    finally:
        stop_event.set()


def _pump_sock_to_ws(
    ws: WebSocketLike, sock: SocketLike, stop_event: threading.Event, session_id: str
) -> None:
    """Forward bytes from the local socket to the WebSocket until either side closes."""
    try:
        while not stop_event.is_set():
            data = sock.recv(_SOCKET_READ_SIZE)
            if not data:
                break
            ws.send(data)
    except Exception:
        logger.info(f"SSH tunnel {session_id}: sock->ws pump ended")
    finally:
        stop_event.set()


def run_bridge(ws: WebSocketLike, sock: SocketLike, session_id: str) -> None:
    """Bridge a connected WebSocket and a connected local socket bidirectionally.

    Returns once either direction closes; tears both ends down before returning.
    """
    stop_event = threading.Event()
    threads = [
        threading.Thread(
            target=_pump_ws_to_sock,
            args=(ws, sock, stop_event, session_id),
            name=f"ssh-tunnel-ws2sock-{session_id}",
            daemon=True,
        ),
        threading.Thread(
            target=_pump_sock_to_ws,
            args=(ws, sock, stop_event, session_id),
            name=f"ssh-tunnel-sock2ws-{session_id}",
            daemon=True,
        ),
    ]
    for t in threads:
        t.start()

    # Block until one direction signals teardown, then close both ends so the other unblocks.
    stop_event.wait()
    logger.info(f"SSH tunnel {session_id}: tearing down")
    try:
        sock.close()
    except Exception:
        logger.debug(f"SSH tunnel {session_id}: error closing local socket", exc_info=True)
    try:
        ws.close()
    except Exception:
        logger.debug(f"SSH tunnel {session_id}: error closing websocket", exc_info=True)

    for t in threads:
        t.join(timeout=5)


class SshTunnelManager:
    """Tracks active SSH tunnel sessions and enforces the per-device limits.

    A single instance is owned by the IoTHubClient. Validation is synchronous (so the direct-method
    ack can report acceptance), while the dial-out and byte pump run on a daemon thread.
    """

    def __init__(
        self,
        ws_connect: WsConnect | None = None,
        sock_connect: SockConnect | None = None,
    ) -> None:
        self._ws_connect = ws_connect or _default_ws_connect
        self._sock_connect = sock_connect or _default_sock_connect
        self._lock = threading.Lock()
        self._active: dict[str, threading.Thread] = {}

    @property
    def _max_sessions(self) -> int:
        try:
            return int(root_cfg.system_cfg.ssh_tunnel_max_sessions)
        except (ValueError, TypeError):
            return 0

    def _validate(self, payload: dict[str, Any]) -> tuple[bool, str | None]:
        """Validate an open request. Returns (accepted, reason). Reason is set when declined."""
        session_id = payload.get("sessionId")
        token = payload.get("token")
        wss_url = payload.get("wssUrl")
        if not session_id or not token or not wss_url:
            return False, "missing sessionId, token or wssUrl"

        # Require TLS: the device must never be downgraded to a plaintext ws:// connection.
        if urlparse(wss_url).scheme != "wss":
            return False, "wssUrl must use the wss:// (TLS) scheme"

        expires_at = _parse_expires_at(payload.get("expiresAt", ""))
        if expires_at is None or expires_at <= datetime.now(UTC):
            return False, "missing, invalid, or expired expiresAt"

        with self._lock:
            self._reap()
            if len(self._active) >= self._max_sessions:
                return False, "device at maximum concurrent sessions"

        return True, None

    def _reap(self) -> None:
        """Drop finished session threads. Caller must hold the lock."""
        for sid in [sid for sid, t in self._active.items() if not t.is_alive()]:
            del self._active[sid]

    def open(self, payload: dict[str, Any]) -> tuple[bool, str | None]:
        """Validate and, if accepted, start the dial-out on a daemon thread.

        Returns the (accepted, reason) tuple to put in the direct-method response.
        """
        accepted, reason = self._validate(payload)
        if not accepted:
            session_id = payload.get("sessionId", "<unknown>")
            logger.warning(f"SSH tunnel {session_id}: declined ({reason})")
            return False, reason

        session_id = str(payload["sessionId"])
        thread = threading.Thread(
            target=self._run_session,
            args=(payload,),
            name=f"ssh-tunnel-{session_id}",
            daemon=True,
        )
        with self._lock:
            self._active[session_id] = thread
        thread.start()
        logger.info(f"SSH tunnel {session_id}: accepted, dialing out")
        return True, None

    def _run_session(self, payload: dict[str, Any]) -> None:
        session_id = str(payload["sessionId"])
        token = str(payload["token"])
        wss_url = str(payload["wssUrl"])
        affinity = payload.get("affinity") or {}
        target_port = int(payload.get("targetPort") or root_cfg.system_cfg.ssh_tunnel_default_port)

        headers = {
            "X-Tunnel-Session": session_id,
            "X-Tunnel-Token": token,
        }
        affinity_name = affinity.get("name")
        affinity_value = affinity.get("value")
        if affinity_name and affinity_value:
            headers["Cookie"] = f"{affinity_name}={affinity_value}"

        ws: WebSocketLike | None = None
        sock: SocketLike | None = None
        try:
            logger.info(f"SSH tunnel {session_id}: connecting WebSocket")
            ws = self._ws_connect(wss_url, headers)
            logger.info(f"SSH tunnel {session_id}: connecting local sshd on port {target_port}")
            sock = self._sock_connect("127.0.0.1", target_port)
            run_bridge(ws, sock, session_id)
        except Exception:
            logger.exception(f"SSH tunnel {session_id}: session failed")
            # run_bridge handles its own teardown; only clean up if we failed before reaching it.
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    logger.debug(f"SSH tunnel {session_id}: error closing socket", exc_info=True)
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    logger.debug(f"SSH tunnel {session_id}: error closing ws", exc_info=True)
        finally:
            with self._lock:
                self._active.pop(session_id, None)
            logger.info(f"SSH tunnel {session_id}: session ended")

    def close_all(self) -> None:
        """Best-effort teardown of all active sessions (used on device shutdown)."""
        with self._lock:
            threads = list(self._active.values())
        for t in threads:
            t.join(timeout=1)
