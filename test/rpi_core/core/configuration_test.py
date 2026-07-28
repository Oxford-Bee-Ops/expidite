import logging
import tempfile
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


class Test_platform_detection:
    """Platform discovery, which runs at import time and so takes down every import if it fails."""

    # The strings platform.platform() / platform.system() / platform.node() report on each platform.
    WINDOWS = ("Windows-11-10.0.26200-SP0", "Windows", "dev-laptop")
    MACOS = ("macOS-26.5.1-arm64-arm-64bit-Mach-O", "Darwin", "mac-studio")
    MACOS_FALLBACK = ("Darwin-25.0.0-arm64-arm-64bit", "Darwin", "mac-studio")
    RPI = ("Linux-6.6.51+rpt-rpi-2712-aarch64-with-glibc2.36", "Linux", "bee-ops-1")
    DOCKER = ("Linux-6.8.0-45-generic-x86_64-with-glibc2.39", "Linux", "container")
    AZURE = ("Linux-6.8.0-1017-azure-x86_64-with-glibc2.39", "Linux", "fv-az123-456")

    @pytest.mark.unittest
    def test_macos_is_a_development_platform(self) -> None:
        """Issue #23: importing expidite on a Mac raised NotImplementedError: Unknown platform."""
        logger.info("Run test_macos_is_a_development_platform test")
        for platform_strings in (self.MACOS, self.MACOS_FALLBACK):
            flags = root_cfg._detect_platform(*platform_strings)
            assert flags.macos is True
            assert flags.rpi is False
            assert flags.linux is False

    @pytest.mark.unittest
    def test_every_platform_is_recognised(self) -> None:
        logger.info("Run test_every_platform_is_recognised test")
        assert root_cfg._detect_platform(*self.WINDOWS).windows is True
        assert root_cfg._detect_platform(*self.DOCKER) == root_cfg._PlatformFlags(linux=True)
        assert root_cfg._detect_platform(*self.RPI).rpi is True
        # The Azure runners are Linux, so they are detected as such; the fv-az node check is the fallback.
        assert root_cfg._detect_platform(*self.AZURE).linux is True
        assert root_cfg._detect_platform("SomeOS-1.0", "SomeOS", "fv-az123-456").azure is True

    @pytest.mark.unittest
    def test_unknown_platform_still_raises(self) -> None:
        logger.info("Run test_unknown_platform_still_raises test")
        with pytest.raises(NotImplementedError):
            root_cfg._detect_platform("Java-11-openjdk", "Java", "somewhere")

    @pytest.mark.parametrize(
        ("model", "expected_rpi5", "expected_pi_zero"),
        [
            ("Raspberry Pi 5 Model B Rev 1.0", True, False),
            ("Raspberry Pi Zero 2 W Rev 1.0", False, True),
            ("Raspberry Pi 4 Model B Rev 1.5", False, False),
        ],
    )
    @pytest.mark.unittest
    def test_pi_model_detection(
        self, model: str, expected_rpi5: bool, expected_pi_zero: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        logger.info("Run test_pi_model_detection test")
        # /proc/device-tree/model only exists on a Pi, so stub out the read.
        monkeypatch.setattr(root_cfg, "_get_pi_model", lambda: model)
        flags = root_cfg._detect_platform(*self.RPI)
        assert (flags.linux, flags.rpi) == (True, True)
        assert flags.rpi5 is expected_rpi5
        assert flags.pi_zero is expected_pi_zero

    @pytest.mark.unittest
    def test_development_platforms_get_a_working_directory(self) -> None:
        """Detecting macOS is only half the fix - the directory block must handle it too (it raised)."""
        logger.info("Run test_development_platforms_get_a_working_directory test")
        # The directory block runs at import; on this machine one of these is the branch that was taken.
        assert root_cfg.ROOT_WORKING_DIR.exists()
        assert root_cfg.TMP_DIR.exists()
        if root_cfg.running_on_windows or root_cfg.running_on_macos:
            # Development mode: a per-run working dir under the system temp dir, spool inside it.
            assert root_cfg.ROOT_WORKING_DIR.is_relative_to(Path(tempfile.gettempdir()))
            assert root_cfg.SPOOL_DIR.is_relative_to(root_cfg.ROOT_WORKING_DIR)


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
