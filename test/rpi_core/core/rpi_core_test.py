from time import sleep

import pytest
import pytest_socket

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.example import my_fleet_config
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.utils.rpi_emulator import RpiEmulator

logger = root_cfg.setup_logger("expidite")


class Test_SensorFactory:
    @pytest.mark.unittest
    def test_RpiCore_status(self) -> None:
        sc = RpiCore()
        sc.configure(my_fleet_config.INVENTORY)
        message = sc.status()
        logger.info(message)
        assert message is not None

    @pytest.mark.unittest
    def test_RpiCore_cycle(self) -> None:
        logger.info("Run test_RpiCore_cycle test")
        # Standard flow
        # We reset cfg.my_device_id to override the computers mac_address
        # This is a test device defined to have a DummySensor.
        with RpiEmulator.get_instance() as th:
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(my_fleet_config.INVENTORY)

            root_cfg.update_my_device_id("d01111111111")

            # Test starting RpiCore without internet access
            pytest_socket.disable_socket()
            sc = RpiCore()
            sc.configure(inventory)
            sc.start()
            sleep(2)
            sc.status()

            # Re-enable internet access and check status
            pytest_socket.enable_socket()
            sleep(2)
            sc.status()
            sc.stop()
            sc.status()

            # Start again
            sc.start()
            sc.stop()
