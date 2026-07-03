import logging
from pathlib import Path

import pytest

from expidite_rpi.core import api, config_validator
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.example import my_fleet_config

logger = root_cfg.setup_logger("expidite")
root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING


class Test_configuration:
    @pytest.mark.parametrize(
        ("test_input", "expected"),
        [
            ("('d01111111111','name')", "DUMMY"),
        ],
    )
    @pytest.mark.unittest
    def test_get_field(self, test_input: str, expected: str) -> None:
        logger.info("Run test_get_field test")
        _, key = eval(test_input)
        assert root_cfg.my_device.get_field(key) == expected

    @pytest.mark.unittest
    def test_display_cfg(self) -> None:
        logger.info("Run test_display_cfg test")
        assert root_cfg.my_device.display() != ""

    @pytest.mark.unittest
    def test_config_validator(self) -> None:
        logger.info("Run test_config_validator test")
        # Check the configuration is valid
        dptrees = my_fleet_config.create_example_device()
        is_valid, error_message = config_validator.validate_trees(dptrees)
        assert is_valid, error_message


class Test_shutdown_fault_filter:
    """The RAISE_WARNING fault suppression during graceful shutdown."""

    @staticmethod
    def _record(level: int, msg: str, pathname: str = __file__) -> logging.LogRecord:
        # record.module is derived from the pathname stem, which the filter uses to exempt reboot.py.
        return logging.LogRecord("bee_ops", level, pathname, 1, msg, None, None)

    @pytest.mark.unittest
    def test_fault_suppressed_only_while_stopping(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flag = tmp_path / "STOP_EXPIDITE_FLAG"
        monkeypatch.setattr(root_cfg, "STOP_EXPIDITE_FLAG", flag)
        filt = root_cfg._shutdown_fault_filter
        fault = self._record(logging.ERROR, f"{api.RAISE_WARN_TAG}_dev: Error in RpicamSensor")

        # Not shutting down: the fault must be logged as normal.
        assert flag.exists() is False
        assert filt.filter(fault) is True

        # Graceful stop in progress: the fault record is dropped entirely.
        flag.touch()
        assert filt.filter(fault) is False

    @pytest.mark.unittest
    def test_non_faults_and_info_always_pass(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        flag = tmp_path / "STOP_EXPIDITE_FLAG"
        flag.touch()  # even while stopping...
        monkeypatch.setattr(root_cfg, "STOP_EXPIDITE_FLAG", flag)
        filt = root_cfg._shutdown_fault_filter

        # ...a warning without the fault tag is real signal and must survive.
        assert filt.filter(self._record(logging.WARNING, "camera settled slowly")) is True
        # ...and an INFO line that merely mentions the tag is below the fault threshold and survives.
        assert filt.filter(self._record(logging.INFO, f"{api.RAISE_WARN_TAG}_dev: fyi")) is True

    @pytest.mark.unittest
    def test_reboot_module_faults_survive_shutdown(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # reboot.py's own faults report on the shutdown itself and must NOT be suppressed during a stop.
        flag = tmp_path / "STOP_EXPIDITE_FLAG"
        flag.touch()
        monkeypatch.setattr(root_cfg, "STOP_EXPIDITE_FLAG", flag)
        filt = root_cfg._shutdown_fault_filter

        reboot_fault = self._record(
            logging.ERROR,
            f"{api.RAISE_WARN_TAG}_dev: RpiCore did not stop within 240s; rebooting anyway",
            pathname="/x/expidite_rpi/core/reboot.py",
        )
        assert filt.filter(reboot_fault) is True
