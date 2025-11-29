from time import sleep

import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import DeviceCfg
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.sensors import device_recipes
from expidite_rpi.sensors.sensor_rpicam_vid import RPICAM_DATA_TYPE_ID
from expidite_rpi.utils.rpi_emulator import RpiTestRecording

logger = root_cfg.setup_logger("expidite")

root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING


class Test_trap_cam_device:
    @pytest.fixture
    def inventory(self):
        return [
            DeviceCfg(
                name="Alex",
                device_id="d01111111111",  # This is the DUMMY MAC address for windows
                notes="Testing trap camera device",
                dp_trees_create_method=device_recipes.create_trapcam_device,
            ),
        ]

    @pytest.mark.unittest
    def test_trap_cam_device(self, rpi) -> None:
        logger.info("Running test_trap_cam_device")

        # Set the file to be fed into the trap camera device
        rpi.set_recordings(
            [
                RpiTestRecording(
                    cmd_prefix="rpicam-vid",
                    recordings=[
                        root_cfg.TEST_DIR
                        / "rpi_core"
                        / "sensors"
                        / "resources"
                        / "V3_TRAPCAM_Bees_in_a_tube.mp4"
                    ],
                )
            ]
        )

        # Limit the RpiCore to 1 recording so we can easily validate the results
        rpi.set_recording_cap(1)

        # Configure RpiCore with the trap camera device
        sc = RpiCore()
        sc.configure(rpi.inventory)
        sc.start()
        while not rpi.recordings_cap_hit(type_id=RPICAM_DATA_TYPE_ID):
            sleep(1)
        while rpi.recordings_still_to_process():
            sleep(1)
        sleep(3)
        sc.stop()
        sleep(3)

        # We should have identified bees in the video and save the info to the EXITCAM datastream
        rpi.assert_records("expidite-fair", {"V3_*": 1})
        rpi.assert_records("expidite-journals", {"*": 0})
        rpi.assert_records("expidite-upload", {"V3_TRAPCAM*": 1})
        rpi.assert_records(
            "expidite-system-records", {"V3_SCORE": 1, "V3_SCORP": 1, "V3_HEART": 1, "V3_WARNING": 0}
        )
        score_df = rpi.get_journal_as_df("expidite-system-records", "V3_SCORE*")
        # Groupby observed_type_id
        grouped_df = score_df.groupby("observed_type_id").agg(
            {
                "count": "sum",
            }
        )
        assert len(grouped_df) > 0, "No records found in the score datastream"
        assert grouped_df.loc["RPICAM", "count"] == 1, "RPICAM count is not 1"
        assert grouped_df.loc["TRAPCAM", "count"] == 1, "TRAPCAM count is not 1"
