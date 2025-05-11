import datetime as dt
from datetime import datetime

import pytest
from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.utils import utils

logger = root_cfg.setup_logger("sensor_core")


class Test_utils:
    @pytest.mark.quick
    def test_display_cfg(self) -> None:
        assert root_cfg.my_device.display() != ""

    @pytest.mark.quick
    def test_utc_to_str(self) -> None:
        timestamp = api.utc_to_fname_str()
        assert len(timestamp) == len("20250101T010101000"), "Invalid timestamp length:" + timestamp

    @pytest.mark.quick
    def test_utc_now(self) -> None:
        # Get datetime_now and convert to a POSIX timestamp (float)
        dt_object = api.utc_now()
        dt_float = dt_object.timestamp()
        ts_of_float = str(datetime.fromtimestamp(dt_float, dt.UTC))
        ts_of_dt = str(dt.datetime.now(dt.UTC))
        print(
            "dt_float:" + str(dt_float) + " dt_float=>" + ts_of_float + "; dt.now()=>",
            ts_of_dt,
        )
        assert ts_of_float[:19] == ts_of_dt[:19], "ts_of_float and ts_of_dt are not the same"

        # Get our standard UTC timestamp and then convert it back to a POSIX timestamp (float)
        ts_of_utils_utc = api.str_to_iso(api.utc_to_fname_str(dt_float))
        print("ts_of_utils_utc:", ts_of_utils_utc, " ts_of_dt:", ts_of_dt)


    @pytest.mark.quick
    def test_raise_warn(self) -> None:
        logmsg = root_cfg.RAISE_WARN() + "This is a test error message"
        logger.error(logmsg)
        assert logmsg.startswith(api.RAISE_WARN_TAG)

    @pytest.mark.quick
    def test_is_sampling_period(self) -> None:
        assert (utils.is_sampling_period(0.5, 180, datetime(2023, 7, 27, 10, 0, 1))) == (
            utils.is_sampling_period(0.5, 180, datetime(2023, 7, 27, 10, 0, 2))
        )
        assert (utils.is_sampling_period(0.5, 180, datetime(2023, 7, 27, 10, 0, 0))) == (
            utils.is_sampling_period(0.5, 180, datetime(2023, 7, 27, 10, 0, 2))
        )

        assert utils.is_sampling_period(0.5, 180, datetime(2023, 7, 27, 11, 0, 14)) is False
        assert utils.is_sampling_period(0.5, 180, datetime(2023, 7, 27, 1, 0, 27)) is True
        # Sampling outside sampling window == False; would normally be True as per test above
        assert (
            utils.is_sampling_period(0.5, 180, datetime(2023, 7, 27, 1, 0, 27), ("06:00", "18:00")) is False
        )
