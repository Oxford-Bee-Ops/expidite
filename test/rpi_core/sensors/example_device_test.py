import logging
from time import sleep

import pytest

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import DeviceCfg
from expidite_rpi.example.my_fleet_config import create_example_device
from expidite_rpi.example.my_processor_example import EXAMPLE_DF_TYPE_ID
from expidite_rpi.example.my_sensor_example import EXAMPLE_FILE_TYPE_ID, EXAMPLE_LOG_TYPE_ID
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.utils.rpi_emulator import RpiEmulator

logger = root_cfg.setup_logger("expidite", level=logging.DEBUG)

root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING


class Test_example_device:
    @pytest.fixture
    def inventory(self) -> list[DeviceCfg]:
        return [
            DeviceCfg(
                name="Alex",
                device_id="d01111111111",  # This is the DUMMY MAC address for windows
                notes="Testing example camera device",
                dp_trees_create_method=create_example_device,
                tags={"Row": "132", "Column": "2", "Location": "Gantry"},
            ),
        ]

    @pytest.mark.unittest
    def test_example_device(self, rpi_emulator: RpiEmulator) -> None:
        logger.info("Running test_example_device")

        # Limit the RpiCore to 3 recordings so we can easily validate the results
        rpi_emulator.set_recording_cap(3)

        # Configure RpiCore with the test device
        sc = RpiCore()
        sc.configure(rpi_emulator.inventory)
        sc.start()
        while not rpi_emulator.recordings_cap_hit(type_id=EXAMPLE_FILE_TYPE_ID):
            sleep(1)
        while rpi_emulator.recordings_still_to_process():
            sleep(1)
        sc.stop()

        # The example sensor produces:
        # - a stream of jpg files (EXAMPLE_FILE_DS_TYPE_ID)
        # - a stream of logs (EXAMPLE_LOG_DS_TYPE_ID).
        # We save 100% of jpg file samples from the example sensor to expidite-upload
        # but the originals all get deleted after processing by the example processor.
        # The example processor takes the jpg files and saves:
        # - a df stream with "pixel_count" (EXAMPLE_DF_DS_TYPE_ID).
        rpi_emulator.assert_records("expidite-fair", {"V3_*": 1})
        rpi_emulator.assert_records("expidite-journals", {"V3_DUMML*": 1, "V3_DUMMD*": 1})
        rpi_emulator.assert_records("expidite-upload", {"V3_DUMMF*": 3})
        df = rpi_emulator.get_journal_as_df("expidite-journals", "V3_DUMMD*")
        assert df is not None, "Expected df to be not None"
        for field in api.ALL_RECORD_ID_FIELDS:
            assert field in df.columns, "Expected all of the api.RECORD_ID fields, missing: " + field
        assert (df["version_id"] == "V3").all()
        assert (df["data_type_id"] == EXAMPLE_DF_TYPE_ID).all()
        assert (df["device_id"] == root_cfg.DUMMY_MAC).all()
        assert (df["sensor_index"] == 1).all()
        assert (df["stream_index"] == 0).all()
        assert (df["pixel_count"] == 25).all(), "Expected pixel_count to be 25"
        assert (df["timestamp"].str.contains("T")).all(), "Expected timestamp to contain T"

        df_log = rpi_emulator.get_journal_as_df("expidite-journals", "V3_DUMML*")
        assert df_log is not None, "Expected df_log to be not None"
        assert df_log["temperature"].max() == len(df_log), (
            f"value_ticker {df_log['temperature'].max()} = len(df_log) {len(df_log)}"
        )

        score_df = rpi_emulator.get_journal_as_df("expidite-system-records", "V3_SCORE*")
        grouped_df = score_df.groupby("observed_type_id").agg({"count": "sum"}).reset_index()
        assert len(grouped_df) > 0, "No records found in the score datastream"
        # Select the row with the observed_type_id of EXAMPLE_DF_DS_TYPE_ID and get the count
        d_count = grouped_df.loc[grouped_df["observed_type_id"] == EXAMPLE_DF_TYPE_ID, "count"].values[0]
        f_count = grouped_df.loc[grouped_df["observed_type_id"] == EXAMPLE_FILE_TYPE_ID, "count"].values[0]
        l_count = grouped_df.loc[grouped_df["observed_type_id"] == EXAMPLE_LOG_TYPE_ID, "count"].values[0]
        assert d_count == 3, "Expected 3 df records"
        assert f_count == 3, "Expected 3 file records"
        assert l_count == len(df_log), f"Expected SCORE log count {l_count} to equal rows {len(df_log)}"
