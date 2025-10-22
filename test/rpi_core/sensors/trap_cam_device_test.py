from time import sleep

import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import DeviceCfg
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.sensors import device_recipes
from expidite_rpi.sensors.sensor_rpicam_vid import RPICAM_DATA_TYPE_ID
from expidite_rpi.utils.rpi_emulator import RpiEmulator, RpiTestRecording

logger = root_cfg.setup_logger("expidite")

root_cfg.TEST_MODE = root_cfg.MODE.TEST

INVENTORY: list[DeviceCfg] = [
    DeviceCfg(
        name="Alex",
        device_id="d01111111111",  # This is the DUMMY MAC address for windows
        notes="Testing trap camera device",
        dp_trees_create_method=device_recipes.create_trapcam_device,
    ),
]

class Test_trap_cam_device:

    @pytest.mark.unittest
    def test_trap_cam_device(self):
        logger.info("Running test_trap_cam_device")

        with RpiEmulator.get_instance() as th:
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(INVENTORY)

            # Set the file to be fed into the trap camera device
            th.set_recordings([
                RpiTestRecording(
                    cmd_prefix="rpicam-vid",
                    recordings=[
                        root_cfg.TEST_DIR / "rpi_core" / "sensors" / "resources" / 
                        "V3_TRAPCAM_Bees_in_a_tube.mp4"
                    ],
                )
            ])

            # Limit the RpiCore to 1 recording so we can easily validate the results
            th.set_recording_cap(1)

            # Configure RpiCore with the trap camera device
            sc = RpiCore()
            sc.configure(inventory)
            sc.start()
            while not th.recordings_cap_hit(type_id=RPICAM_DATA_TYPE_ID):
                sleep(1)
            while th.recordings_still_to_process():
                sleep(1)
            sleep(3)
            sc.stop()
            sleep(3)

            # We should have identified bees in the video and save the info to the EXITCAM datastream
            th.assert_records("expidite-fair", 
                            {"V3_*": 1})
            th.assert_records("expidite-journals", 
                            {"*": 0})
            th.assert_records("expidite-upload", 
                            {"V3_TRAPCAM*": 1})
            th.assert_records("expidite-system-records", 
                            {"V3_SCORE": 1, "V3_SCORP": 1, "V3_HEART": 1, "V3_WARNING": 0})
            score_df = th.get_journal_as_df("expidite-system-records", "V3_SCORE*")
            # Groupby observed_type_id
            grouped_df = score_df.groupby("observed_type_id").agg({"count": "sum",})
            assert len(grouped_df) > 0, "No records found in the score datastream"
            assert grouped_df.loc["RPICAM", "count"] == 1, "RPICAM count is not 1"
            assert grouped_df.loc["TRAPCAM", "count"] == 1, "TRAPCAM count is not 1"