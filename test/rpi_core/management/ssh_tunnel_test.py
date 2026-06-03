from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import expidite_rpi.core.configuration as root_cfg
from expidite_rpi.management.ssh_tunnel import (
    SshTunnelManager,
    _parse_expires_at,
    run_bridge,
)

logger = root_cfg.setup_logger("expidite")


def _future_iso(seconds: int = 60) -> str:
    return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _valid_payload(**overrides: object) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sessionId": "sess-1",
        "token": "secret-token",
        "wssUrl": "wss://portal.example.net/internal/ssh-tunnel",
        "affinity": {"name": "ARRAffinity", "value": "inst-123"},
        "targetPort": 22,
        "expiresAt": _future_iso(),
    }
    payload.update(overrides)
    return payload


@pytest.fixture(autouse=True)
def enable_tunnel(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set a default session cap and target port unless a test overrides them."""
    monkeypatch.setattr(root_cfg.system_cfg, "ssh_tunnel_max_sessions", "3")
    monkeypatch.setattr(root_cfg.system_cfg, "ssh_tunnel_default_port", "22")


# ---- Helper parsing ----


class TestParsing:
    @pytest.mark.unittest
    def test_parse_expires_at_handles_z_suffix(self) -> None:
        parsed = _parse_expires_at("2030-01-01T00:00:00Z")
        assert parsed is not None
        assert parsed.tzinfo is not None

    @pytest.mark.unittest
    def test_parse_expires_at_invalid(self) -> None:
        assert _parse_expires_at("not-a-date") is None


# ---- Validation ----


class TestValidation:
    @pytest.mark.unittest
    def test_accepts_valid(self) -> None:
        mgr = SshTunnelManager()
        accepted, reason = mgr._validate(_valid_payload())
        assert accepted is True
        assert reason is None

    @pytest.mark.unittest
    def test_rejects_missing_fields(self) -> None:
        mgr = SshTunnelManager()
        accepted, reason = mgr._validate(_valid_payload(token=""))
        assert accepted is False
        assert reason is not None

    @pytest.mark.unittest
    def test_rejects_non_tls_scheme(self) -> None:
        mgr = SshTunnelManager()
        accepted, reason = mgr._validate(_valid_payload(wssUrl="ws://portal.example.net/internal/ssh-tunnel"))
        assert accepted is False
        assert reason == "wssUrl must use the wss:// (TLS) scheme"

    @pytest.mark.unittest
    def test_rejects_expired(self) -> None:
        mgr = SshTunnelManager()
        accepted, reason = mgr._validate(_valid_payload(expiresAt=_future_iso(seconds=-10)))
        assert accepted is False
        assert reason == "missing, invalid, or expired expiresAt"

    @pytest.mark.unittest
    def test_rejects_at_max_sessions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(root_cfg.system_cfg, "ssh_tunnel_max_sessions", "1")
        mgr = SshTunnelManager()
        # Simulate one live session occupying the only slot.
        live = threading.Thread(target=lambda: threading.Event().wait(0.5))
        live.start()
        mgr._active["existing"] = live
        try:
            accepted, reason = mgr._validate(_valid_payload())
            assert accepted is False
            assert reason == "device at maximum concurrent sessions"
        finally:
            live.join()


# ---- Bridge ----


class _FakeWs:
    """Minimal WebSocketLike: yields queued messages then blocks until released, then EOFs.

    ``drained`` is set once every queued message has been returned (and therefore forwarded by the
    pump). ``release`` lets the test hold off EOF until both directions have drained, making the
    bidirectional bridge test deterministic.
    """

    def __init__(self, incoming: list[bytes], release: threading.Event) -> None:
        self._incoming = list(incoming)
        self._release = release
        self.drained = threading.Event()
        self.sent: list[bytes] = []
        self.closed = False

    def recv(self, timeout: float | None = None) -> bytes:
        _ = timeout
        if self._incoming:
            return self._incoming.pop(0)
        self.drained.set()
        self._release.wait(5)
        msg = "ws closed"
        raise ConnectionError(msg)

    def send(self, message: bytes | str) -> None:
        if self.closed:
            msg = "ws closed"
            raise ConnectionError(msg)
        self.sent.append(message if isinstance(message, bytes) else message.encode())

    def close(self, code: int = 1000, reason: str = "") -> None:
        _ = (code, reason)
        self.closed = True


class _FakeSock:
    """Minimal SocketLike: yields queued chunks then blocks until released, then EOFs."""

    def __init__(self, incoming: list[bytes], release: threading.Event) -> None:
        self._incoming = list(incoming)
        self._release = release
        self.drained = threading.Event()
        self.sent: list[bytes] = []
        self.closed = False

    def recv(self, bufsize: int) -> bytes:
        _ = bufsize
        if self._incoming:
            return self._incoming.pop(0)
        self.drained.set()
        self._release.wait(5)
        return b""  # EOF

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)

    def close(self) -> None:
        self.closed = True


class TestBridge:
    @pytest.mark.unittest
    def test_run_bridge_pumps_both_directions_and_tears_down(self) -> None:
        release = threading.Event()
        ws = _FakeWs(incoming=[b"from-portal-1", b"from-portal-2"], release=release)
        sock = _FakeSock(incoming=[b"from-pi-1"], release=release)

        t = threading.Thread(target=run_bridge, args=(ws, sock, "sess-1"), daemon=True)
        t.start()
        # Wait until both queues are fully forwarded before allowing either side to EOF.
        assert ws.drained.wait(5)
        assert sock.drained.wait(5)
        release.set()
        t.join(5)
        assert not t.is_alive()

        # ws -> sock
        assert sock.sent == [b"from-portal-1", b"from-portal-2"]
        # sock -> ws
        assert ws.sent == [b"from-pi-1"]
        # Both ends closed on teardown.
        assert ws.closed is True
        assert sock.closed is True

    @pytest.mark.unittest
    def test_open_accepted_starts_and_completes_session(self) -> None:
        release = threading.Event()
        ws = _FakeWs(incoming=[b"hi"], release=release)
        sock = _FakeSock(incoming=[b"there"], release=release)

        def ws_connect(url: str, headers: dict[str, str]) -> _FakeWs:
            # Secrets must be in headers, not the URL.
            assert "X-Tunnel-Token" in headers
            assert headers["Cookie"] == "ARRAffinity=inst-123"
            assert "token" not in url
            return ws

        def sock_connect(host: str, port: int) -> _FakeSock:
            assert host == "127.0.0.1"
            assert port == 22
            return sock

        mgr = SshTunnelManager(ws_connect=ws_connect, sock_connect=sock_connect)
        accepted, reason = mgr.open(_valid_payload())
        assert accepted is True
        assert reason is None

        # Let the session run, then release both ends so it tears down.
        assert ws.drained.wait(5)
        assert sock.drained.wait(5)
        release.set()

        # The daemon session thread should remove itself from _active once finished.
        for _ in range(250):
            if "sess-1" not in mgr._active:
                break
            threading.Event().wait(0.02)
        assert "sess-1" not in mgr._active


# ---- Handler dispatch ----


class TestHandlerDispatch:
    @pytest.mark.unittest
    def test_open_ssh_tunnel_handler_returns_accepted(self) -> None:
        from unittest.mock import MagicMock

        from expidite_rpi.management.iot_hub_client import IoTHubClient

        client = IoTHubClient.__new__(IoTHubClient)
        client._hub_client = MagicMock()
        client.im = MagicMock()
        client.ssh_tunnel_manager = MagicMock()
        client.ssh_tunnel_manager.open.return_value = (True, None)

        result = client._handle_open_ssh_tunnel(_valid_payload())
        assert result == {"accepted": True}
        client.ssh_tunnel_manager.open.assert_called_once()

    @pytest.mark.unittest
    def test_open_ssh_tunnel_handler_returns_reason_when_declined(self) -> None:
        from unittest.mock import MagicMock

        from expidite_rpi.management.iot_hub_client import IoTHubClient

        client = IoTHubClient.__new__(IoTHubClient)
        client._hub_client = MagicMock()
        client.im = MagicMock()
        client.ssh_tunnel_manager = MagicMock()
        client.ssh_tunnel_manager.open.return_value = (False, "device at maximum concurrent sessions")

        result = client._handle_open_ssh_tunnel(_valid_payload())
        assert result["accepted"] is False
        assert result["reason"] == "device at maximum concurrent sessions"
