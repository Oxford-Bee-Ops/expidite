from types import SimpleNamespace

import pytest

from expidite_rpi.core.device_config_objects import WifiClient
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core import device_manager

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

        monkeypatch.setattr(
            device_manager.root_cfg,
            "my_device",
            SimpleNamespace(wifi_clients=[WifiClient("open-net", 100, "")]),
        )
        monkeypatch.setattr(
            device_manager.utils,
            "run_cmd",
            lambda cmd, ignore_errors=False, grep_strs=None: commands.append(cmd) or "",
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

        monkeypatch.setattr(device_manager, "sleep", lambda _: None)
        monkeypatch.setattr(
            device_manager.utils,
            "run_cmd",
            lambda cmd, ignore_errors=False, grep_strs=None: commands.append(cmd) or "",
        )

        manager.attempt_wifi_recovery()

        assert len(commands) == 1
        assert commands[0] == "sudo nmcli dev wifi connect open-net"
