from time import sleep

import pytest
from sensor_core import DeviceCfg, SensorCore
from sensor_core import configuration as root_cfg
from sensor_core.sensors import device_recipes
from sensor_core.utils.sc_test_emulator import ScEmulator, ScTestRecording

logger = root_cfg.setup_logger("sensor_core")

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

    @pytest.mark.quick
    def test_trap_cam_device(self):

        with ScEmulator.get_instance() as th:
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(INVENTORY)

            # Set the file to be fed into the trap camera device
            th.set_recordings([
                ScTestRecording(
                    cmd_prefix="rpicam-vid",
                    recordings=[
                        root_cfg.TEST_DIR / "sensors" / "resources" / "V3_TRAPCAM_Bees_in_a_tube.mp4"
                    ],
                )
            ])

            # Limit the SensorCore to 1 recording so we can easily validate the results
            th.set_recording_cap(1)

            # Configure SensorCore with the trap camera device
            sc = SensorCore()
            sc.configure(inventory)
            sc.start()
            sleep(10)
            sc.stop()
            sleep(2)

            # We should have identified bees in the video and save the info to the EXITCAM datastream
            th.assert_records("sensor-core-fair", 
                            {"V3_*": 1})
            th.assert_records("sensor-core-journals", 
                            {"*": 0})
            th.assert_records("sensor-core-upload", 
                            {"V3_TRAPCAM*": 1})