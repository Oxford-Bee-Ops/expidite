from types import SimpleNamespace

import pytest

import expidite_rpi.core.configuration as root_cfg
from expidite_rpi.core import device_manager
from expidite_rpi.core.device_config_objects import WifiClient

logger = root_cfg.setup_logger("expidite")


class Test_device_manager:
    @pytest.mark.unittest
    def test_device_manager(self) -> None:
        logger.info("Run test_device_manager test")
        device_manager.DeviceManager()

    @pytest.mark.unittest
    def test_inject_wifi_clients_supports_open_networks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        logger.info("Run test_inject_wifi_clients_supports_open_networks test")
        manager = device_manager.DeviceManager.__new__(device_manager.DeviceManager)
        manager.client_wlan = "wlan0"

        commands: list[str] = []

        def run_cmd_mock(cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None) -> str:
            del ignore_errors, grep_strs
            commands.append(cmd)
            return ""

        monkeypatch.setattr(
            device_manager.root_cfg,
            "my_device",
            SimpleNamespace(wifi_clients=[WifiClient("open-net", 100, "")]),
        )
        monkeypatch.setattr(
            device_manager.utils,
            "run_cmd",
            run_cmd_mock,
        )

        manager.inject_wifi_clients()

        assert len(commands) == 2
        assert "wifi-sec.key-mgmt none" in commands[1]
        assert "wifi-sec.psk" not in commands[1]

    @pytest.mark.unittest
    def test_attempt_wifi_recovery_supports_open_networks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        logger.info("Run test_attempt_wifi_recovery_supports_open_networks test")
        manager = device_manager.DeviceManager.__new__(device_manager.DeviceManager)
        manager.wifi_clients = [WifiClient("open-net", 100, "")]
        manager.ping_failure_count_run = 40
        manager.ping_check_interval = 10.0

        commands: list[str] = []

        def run_cmd_mock(cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None) -> str:
            del ignore_errors, grep_strs
            commands.append(cmd)
            return ""

        monkeypatch.setattr(device_manager, "sleep", lambda _: None)
        monkeypatch.setattr(
            device_manager.utils,
            "run_cmd",
            run_cmd_mock,
        )

        manager.attempt_wifi_recovery()

        assert len(commands) == 1
        assert commands[0] == "sudo nmcli dev wifi connect open-net"
