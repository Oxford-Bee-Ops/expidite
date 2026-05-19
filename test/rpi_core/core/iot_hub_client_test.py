from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import expidite_rpi.core.configuration as root_cfg
from expidite_rpi.core.api import LedsInstalled
from expidite_rpi.core.device_config_objects import FAILED_TO_LOAD, DeviceCfg, WifiClient
from expidite_rpi.core.iot_hub_client import (
    IoTHubClient,
    _derive_device_key,
    device_cfg_to_twin_dict,
    wifi_clients_from_twin,
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


class TestWifiClientsFromTwin:
    @pytest.mark.unittest
    def test_deserialize(self) -> None:
        raw = [
            {"ssid": "MyWifi", "priority": 100, "pw": "secret"},
            {"ssid": "Backup", "priority": 50, "pw": "other"},
        ]
        result = wifi_clients_from_twin(raw)
        assert len(result) == 2
        assert result[0] == WifiClient(ssid="MyWifi", priority=100, pw="secret")
        assert result[1] == WifiClient(ssid="Backup", priority=50, pw="other")

    @pytest.mark.unittest
    def test_empty_list(self) -> None:
        assert wifi_clients_from_twin([]) == []


class TestDeviceCfgToTwinDict:
    @pytest.mark.unittest
    def test_excludes_callables(self) -> None:
        cfg = DeviceCfg(dp_trees_create_method=lambda: None)
        result = device_cfg_to_twin_dict(cfg)
        assert "dp_trees_create_method" not in result
        assert "dp_trees_create_kwargs" not in result

    @pytest.mark.unittest
    def test_serializes_enum(self) -> None:
        cfg = DeviceCfg(leds_installed=LedsInstalled.RED_AND_GREEN)
        result = device_cfg_to_twin_dict(cfg)
        assert result["leds_installed"] == LedsInstalled.RED_AND_GREEN.value

    @pytest.mark.unittest
    def test_serializes_wifi_clients(self) -> None:
        cfg = DeviceCfg(wifi_clients=[WifiClient("net", 10, "pw")])
        result = device_cfg_to_twin_dict(cfg)
        assert result["wifi_clients"] == [{"ssid": "net", "priority": 10, "pw": "pw"}]

    @pytest.mark.unittest
    def test_includes_basic_fields(self) -> None:
        cfg = DeviceCfg(name="test-device", heart_beat_frequency=120)
        result = device_cfg_to_twin_dict(cfg)
        assert result["name"] == "test-device"
        assert result["heart_beat_frequency"] == 120


class TestGetInstance:
    @pytest.mark.unittest
    def test_returns_none_when_no_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        IoTHubClient._instance = None
        monkeypatch.setattr(
            root_cfg,
            "keys",
            SimpleNamespace(
                dps_scope_id=FAILED_TO_LOAD,
                dps_primary_key=FAILED_TO_LOAD,
            ),
        )
        result = IoTHubClient.get_instance()
        assert result is None

    @pytest.mark.unittest
    def test_returns_none_when_partial_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        IoTHubClient._instance = None
        monkeypatch.setattr(
            root_cfg,
            "keys",
            SimpleNamespace(
                dps_scope_id="0ne12345678",
                dps_primary_key=FAILED_TO_LOAD,
            ),
        )
        result = IoTHubClient.get_instance()
        assert result is None

    @pytest.mark.unittest
    @patch("expidite_rpi.core.iot_hub_client.IoTHubDeviceClient", create=True)
    def test_returns_instance_when_credentials_present(
        self, _mock_hub: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        IoTHubClient._instance = None
        monkeypatch.setattr(
            root_cfg,
            "keys",
            SimpleNamespace(
                dps_scope_id="0ne12345678",
                dps_primary_key="dGVzdGtleQ==",
            ),
        )
        result = IoTHubClient.get_instance()
        assert result is not None
        assert isinstance(result, IoTHubClient)
        IoTHubClient._instance = None


class TestDirectMethodHandlers:
    """Test direct method handlers by calling them directly (bypassing SDK dispatch)."""

    def _make_client(self) -> IoTHubClient:
        """Create an IoTHubClient without DPS provisioning."""
        client = IoTHubClient.__new__(IoTHubClient)
        client._hub_client = MagicMock()
        client._report_timer = None
        return client

    @pytest.mark.unittest
    def test_handle_stop_creates_flag(self, tmp_path: pytest.TempPathFactory) -> None:
        client = self._make_client()
        flag = tmp_path / "STOP_FLAG"  # type: ignore[operator]
        with patch.object(root_cfg, "STOP_EXPIDITE_FLAG", flag):
            result = client._handle_stop({"pkill": False})
        assert result["status"] == "stopping"
        assert flag.exists()

    @pytest.mark.unittest
    def test_handle_trigger_sensing_writes_duration(self, tmp_path: pytest.TempPathFactory) -> None:
        client = self._make_client()
        flag = tmp_path / "SENSOR_TRIGGER_FLAG"  # type: ignore[operator]
        with patch.object(root_cfg, "SENSOR_TRIGGER_FLAG", flag):
            result = client._handle_trigger_sensing({"duration": 30})
        assert result["status"] == "sensing_triggered"
        assert result["duration"] == "30"
        assert flag.read_text() == "30"

    @pytest.mark.unittest
    def test_handle_enter_review_mode(self, tmp_path: pytest.TempPathFactory) -> None:
        client = self._make_client()
        review_flag = tmp_path / "REVIEW_FLAG"  # type: ignore[operator]
        restart_flag = tmp_path / "RESTART_FLAG"  # type: ignore[operator]
        with (
            patch.object(root_cfg, "REVIEW_MODE_FLAG", review_flag),
            patch.object(root_cfg, "RESTART_EXPIDITE_FLAG", restart_flag),
        ):
            result = client._handle_enter_review_mode({})
        assert result["status"] == "review_mode_entered"
        assert review_flag.exists()
        assert restart_flag.exists()

    @pytest.mark.unittest
    def test_handle_exit_review_mode(self, tmp_path: pytest.TempPathFactory) -> None:
        client = self._make_client()
        review_flag = tmp_path / "REVIEW_FLAG"  # type: ignore[operator]
        review_flag.write_text("12345")
        restart_flag = tmp_path / "RESTART_FLAG"  # type: ignore[operator]
        with (
            patch.object(root_cfg, "REVIEW_MODE_FLAG", review_flag),
            patch.object(root_cfg, "RESTART_EXPIDITE_FLAG", restart_flag),
        ):
            result = client._handle_exit_review_mode({})
        assert result["status"] == "review_mode_exited"
        assert not review_flag.exists()
        assert restart_flag.exists()

    @pytest.mark.unittest
    def test_handle_reboot_not_on_rpi(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client()
        monkeypatch.setattr(root_cfg, "running_on_rpi", False)
        result = client._handle_reboot({})
        assert "error" in result

    @pytest.mark.unittest
    def test_handle_start_not_on_rpi(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client()
        monkeypatch.setattr(root_cfg, "running_on_rpi", False)
        result = client._handle_start({})
        assert "error" in result

    @pytest.mark.unittest
    def test_handle_get_status(self) -> None:
        client = self._make_client()
        mock_orch = MagicMock()
        mock_orch.status.return_value = {"RpiCore running": "True"}
        with patch(
            "expidite_rpi.core.edge_orchestrator.EdgeOrchestrator.get_instance",
            return_value=mock_orch,
        ):
            result = client._handle_get_status({})
        assert result["RpiCore running"] == "True"

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


class TestTwinPatch:
    def _make_client(self, monkeypatch: pytest.MonkeyPatch) -> IoTHubClient:
        client = IoTHubClient.__new__(IoTHubClient)
        client._hub_client = MagicMock()
        client._report_timer = None
        cfg = DeviceCfg(
            name="test",
            device_id="d01111111111",
            heart_beat_frequency=600,
            log_level=20,
        )
        monkeypatch.setattr(root_cfg, "my_device", cfg)
        monkeypatch.setattr(root_cfg, "running_on_rpi", False)
        return client

    @pytest.mark.unittest
    def test_apply_simple_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client(monkeypatch)
        applied = client._apply_config_patch(
            {
                "heart_beat_frequency": 300,
                "log_level": 10,
            }
        )
        assert "heart_beat_frequency" in applied
        assert "log_level" in applied
        assert root_cfg.my_device.heart_beat_frequency == 300
        assert root_cfg.my_device.log_level == 10

    @pytest.mark.unittest
    def test_apply_wifi_clients(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client(monkeypatch)
        applied = client._apply_config_patch(
            {
                "wifi_clients": [{"ssid": "NewNet", "priority": 100, "pw": "pass123"}],
            }
        )
        assert "wifi_clients" in applied
        assert len(root_cfg.my_device.wifi_clients) == 1
        assert root_cfg.my_device.wifi_clients[0].ssid == "NewNet"

    @pytest.mark.unittest
    def test_apply_leds_installed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client(monkeypatch)
        applied = client._apply_config_patch(
            {
                "leds_installed": LedsInstalled.RED_ONLY.value,
            }
        )
        assert "leds_installed" in applied
        assert root_cfg.my_device.leds_installed == LedsInstalled.RED_ONLY

    @pytest.mark.unittest
    def test_ignores_excluded_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client(monkeypatch)
        applied = client._apply_config_patch(
            {
                "dp_trees_create_method": "should_be_ignored",
                "device_id": "should_be_ignored",
            }
        )
        assert len(applied) == 0

    @pytest.mark.unittest
    def test_ignores_dollar_prefix_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = self._make_client(monkeypatch)
        applied = client._apply_config_patch(
            {
                "$version": 3,
                "heart_beat_frequency": 120,
            }
        )
        assert "$version" not in applied
        assert "heart_beat_frequency" in applied

    @pytest.mark.unittest
    def test_restart_required_fields_touch_flag(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory
    ) -> None:
        client = self._make_client(monkeypatch)
        restart_flag = tmp_path / "RESTART_FLAG"  # type: ignore[operator]
        with patch.object(root_cfg, "RESTART_EXPIDITE_FLAG", restart_flag):
            client._apply_config_patch({"review_mode_frequency": 10})
        assert restart_flag.exists()

    @pytest.mark.unittest
    def test_non_restart_fields_dont_touch_flag(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory
    ) -> None:
        client = self._make_client(monkeypatch)
        restart_flag = tmp_path / "RESTART_FLAG"  # type: ignore[operator]
        with patch.object(root_cfg, "RESTART_EXPIDITE_FLAG", restart_flag):
            client._apply_config_patch({"heart_beat_frequency": 120})
        assert not restart_flag.exists()


class TestReportCurrentState:
    @pytest.mark.unittest
    def test_reports_properties(self, monkeypatch: pytest.MonkeyPatch) -> None:
        client = IoTHubClient.__new__(IoTHubClient)
        client._hub_client = MagicMock()
        client._report_timer = None
        cfg = DeviceCfg(name="test", device_id="d01111111111")
        monkeypatch.setattr(root_cfg, "my_device", cfg)
        monkeypatch.setattr(root_cfg, "get_version_info", lambda: ("1.0", "2.0", "3.13"))

        mock_orch = MagicMock()
        mock_orch.get_status.return_value = SimpleNamespace(value="RUNNING")
        with patch(
            "expidite_rpi.core.edge_orchestrator.EdgeOrchestrator.get_instance",
            return_value=mock_orch,
        ):
            client._report_current_state()

        client._hub_client.patch_twin_reported_properties.assert_called_once()
        reported = client._hub_client.patch_twin_reported_properties.call_args[0][0]
        assert reported["name"] == "test"
        assert reported["expidite_version"] == "1.0"
        assert reported["orchestrator_status"] == "RUNNING"
        assert "last_reported" in reported

    @pytest.mark.unittest
    def test_no_op_when_hub_client_is_none(self) -> None:
        client = IoTHubClient.__new__(IoTHubClient)
        client._hub_client = None
        client._report_timer = None
        client._report_current_state()  # Should not raise
