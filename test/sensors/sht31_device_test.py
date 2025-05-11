from time import sleep

import pytest
from sensor_core import DeviceCfg, SensorCore
from sensor_core import configuration as root_cfg
from sensor_core.sensors.device_recipes import create_sht31_device
from sensor_core.utils.sc_test_emulator import ScEmulator

logger = root_cfg.setup_logger("sensor_core")

INVENTORY: list[DeviceCfg] = [
    DeviceCfg(
        name="Alex",
        device_id="d01111111111",  # This is the DUMMY MAC address for windows
        notes="Testing SHT31 temp / humidity device",
        dp_trees_create_method=create_sht31_device,
    ),
]

class Test_sht31_device:

    @pytest.mark.quick
    def test_sht31_device(self):

        with ScEmulator.get_instance() as th:
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(INVENTORY)

            # Configure SensorCore with the trap camera device
            sc = SensorCore()
            sc.configure(inventory)
            sc.start()
            sleep(2)
            sc.stop()
            sleep(2)
            th.assert_records("sensor-core-fair", 
                            {"V3_*": 1})
            th.assert_records("sensor-core-journals", 
                            {"V3_SHT31*": 1})
