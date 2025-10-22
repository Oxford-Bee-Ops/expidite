from time import sleep

import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import DeviceCfg
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.sensors.device_recipes import create_sht40_device
from expidite_rpi.utils.rpi_emulator import RpiEmulator

logger = root_cfg.setup_logger("expidite")

INVENTORY: list[DeviceCfg] = [
    DeviceCfg(
        name="Alex",
        device_id="d01111111111",  # This is the DUMMY MAC address for windows
        notes="Testing SHT40 temp / humidity device",
        dp_trees_create_method=create_sht40_device,
    ),
]

class Test_SHT40_device:

    @pytest.mark.unittest
    def test_SHT40_device(self):

        logger.info("Running test_SHT40_device")
        with RpiEmulator.get_instance() as th:
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(INVENTORY)

            # Configure RpiCore with the trap camera device
            sc = RpiCore()
            sc.configure(inventory)
            if root_cfg.running_on_windows:
                logger.warning("Skipping SHT40 test on Windows - requires I2C")
                return
            
            sc.start()
            sleep(2)
            sc.stop()
            sleep(2)
            th.assert_records("expidite-fair", 
                            {"V3_*": 1})
            th.assert_records("expidite-journals", 
                            {"V3_SHT40*": 1})
