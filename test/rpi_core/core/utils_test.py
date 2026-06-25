import datetime as dt
from datetime import UTC, datetime

import pytest

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")


class Test_utils:
    @pytest.mark.unittest
    def test_display_cfg(self) -> None:
        logger.info("Run test_display_cfg test")
        assert root_cfg.my_device.display() != ""

    @pytest.mark.unittest
    def test_utc_to_str(self) -> None:
        logger.info("Run test_utc_to_str test")
        timestamp = api.utc_to_fname_str()
        assert len(timestamp) == len("20250101T010101000"), "Invalid timestamp length:" + timestamp

    @pytest.mark.unittest
    def test_utc_now(self) -> None:
        logger.info("Run test_utc_now test")
        # Get datetime_now and convert to a POSIX timestamp (float)
        dt_object = api.utc_now()
        dt_float = dt_object.timestamp()
        ts_of_float = str(datetime.fromtimestamp(dt_float, UTC))
        ts_of_dt = str(dt.datetime.now(UTC))
        print(
            "dt_float:" + str(dt_float) + " dt_float=>" + ts_of_float + "; dt.now()=>",
            ts_of_dt,
        )
        assert ts_of_float[:19] == ts_of_dt[:19], "ts_of_float and ts_of_dt are not the same"

        # Get our standard UTC timestamp and then convert it back to a POSIX timestamp (float)
        ts_of_utils_utc = api.str_to_iso(api.utc_to_fname_str(dt_float))
        print("ts_of_utils_utc:", ts_of_utils_utc, " ts_of_dt:", ts_of_dt)

    @pytest.mark.unittest
    def test_raise_warn(self) -> None:
        logger.info("Run test_raise_warn test")
        logmsg = root_cfg.RAISE_WARN() + "This is a test error message"
        logger.error(logmsg)
        assert logmsg.startswith(api.RAISE_WARN_TAG)

    @pytest.mark.unittest
    def test_run_video_cmd_uses_command_duration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """run_video_cmd should derive its timeout from the command's own -t (milliseconds)."""
        captured: dict[str, float | None] = {}

        def fake_run_cmd(
            cmd: str,
            ignore_errors: bool = False,
            grep_strs: list[str] | None = None,
            timeout: float | None = None,
        ) -> str:
            captured["timeout"] = timeout
            return ""

        monkeypatch.setattr(utils, "run_cmd", fake_run_cmd)
        utils.run_video_cmd("rpicam-vid -o out.mp4 -t 180000", margin_s=60.0)
        # 180000ms = 180s, plus the 60s margin.
        assert captured["timeout"] == pytest.approx(240.0)

    @pytest.mark.unittest
    def test_run_video_cmd_falls_back_to_default_when_no_t(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Commands without a -t (e.g. rpicam-still review frames) get a bounded default timeout."""
        captured: dict[str, float | None] = {}

        def fake_run_cmd(
            cmd: str,
            ignore_errors: bool = False,
            grep_strs: list[str] | None = None,
            timeout: float | None = None,
        ) -> str:
            captured["timeout"] = timeout
            return ""

        monkeypatch.setattr(utils, "run_cmd", fake_run_cmd)
        utils.run_video_cmd("rpicam-still -o out.jpg", default_duration_s=10.0, margin_s=60.0)
        assert captured["timeout"] == pytest.approx(70.0)
