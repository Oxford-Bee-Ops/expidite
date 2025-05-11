from time import sleep

import pytest
from rpi.core import configuration as root_cfg
from rpi.core.device_config_objects import DeviceCfg
from rpi.rpi_core import RpiCore
from rpi.sensors.device_recipes import create_sht31_device
from rpi.utils.rpi_emulator import RpiEmulator

logger = root_cfg.setup_logger("rpi_core")

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

        with RpiEmulator.get_instance() as th:
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(INVENTORY)

            # Configure RpiCore with the trap camera device
            sc = RpiCore()
            sc.configure(inventory)
            sc.start()
            sleep(2)
            sc.stop()
            sleep(2)
            th.assert_records("expidite-fair", 
                            {"V3_*": 1})
            th.assert_records("expidite-journals", 
                            {"V3_SHT31*": 1})
