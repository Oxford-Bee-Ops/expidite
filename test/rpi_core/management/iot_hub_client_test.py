from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import expidite_rpi.core.configuration as root_cfg
from expidite_rpi.management.iot_hub_client import (
    IoTHubClient,
    _derive_device_key,
)

logger = root_cfg.setup_logger("expidite")


class TestDeriveDeviceKey:
    @pytest.mark.unittest
    def test_known_vector(self) -> None:
        """Verify HMAC-SHA256 key derivation produces a deterministic base64 result."""
        key = _derive_device_key("dGVzdGtleQ==", "device001")
        assert isinstance(key, str)
        assert len(key) > 0
        # Same inputs must produce the same output
        assert key == _derive_device_key("dGVzdGtleQ==", "device001")

    @pytest.mark.unittest
    def test_different_device_ids_produce_different_keys(self) -> None:
        group_key = "dGVzdGtleQ=="
        key_a = _derive_device_key(group_key, "deviceA")
        key_b = _derive_device_key(group_key, "deviceB")
        assert key_a != key_b


class TestDirectMethodHandlers:
    """Test direct method handlers by calling them directly (bypassing SDK dispatch)."""

    def _make_client(self) -> IoTHubClient:
        """Create an IoTHubClient without DPS provisioning."""
        client = IoTHubClient.__new__(IoTHubClient)
        client._hub_client = MagicMock()
        client.im = MagicMock()
        return client

    @pytest.mark.unittest
    def test_handle_enter_review_mode(self) -> None:
        client = self._make_client()
        client.im.is_review_mode_enabled.return_value = True  # type: ignore[attr-defined]
        result = client._handle_enter_review_mode({})
        client.im.enter_review_mode.assert_called_once()  # type: ignore[attr-defined]
        assert result["status"] == "True"

    @pytest.mark.unittest
    def test_handle_exit_review_mode(self) -> None:
        client = self._make_client()
        client.im.is_review_mode_enabled.return_value = False  # type: ignore[attr-defined]
        result = client._handle_exit_review_mode({})
        client.im.exit_review_mode.assert_called_once()  # type: ignore[attr-defined]
        assert result["status"] == "False"

    @pytest.mark.unittest
    def test_handle_reboot_not_on_rpi(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client()
        monkeypatch.setattr(root_cfg, "running_on_rpi", False)
        result = client._handle_reboot({})
        assert "error" in result

    @pytest.mark.unittest
    def test_method_dispatch_unknown_method(self) -> None:
        client = self._make_client()
        mock_request = MagicMock()
        mock_request.name = "nonexistent_method"
        mock_request.payload = {}
        with patch("azure.iot.device.MethodResponse") as mock_mr:
            mock_mr.create_from_method_request.return_value = MagicMock()
            client._on_method_request(mock_request)
            mock_mr.create_from_method_request.assert_called_once()
            call_args = mock_mr.create_from_method_request.call_args
            assert call_args[0][1] == 404
