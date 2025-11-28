import logging
from threading import Thread
from time import sleep

import pytest

from expidite_rpi.core import api, edge_orchestrator
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator
from expidite_rpi.example.my_fleet_config import INVENTORY
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.utils import rpi_emulator

logger = root_cfg.setup_logger("expidite", level=logging.DEBUG)

root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING

class Test_Orchestrator:
    @pytest.mark.unittest
    def test_RpiCore_status(self) -> None:
        sc = RpiCore()
        sc.configure(INVENTORY)
        message = sc.status()
        logger.info(message)
        assert message is not None


    @pytest.mark.unittest
    def test_Orchestrator(self) -> None:
        logger.info("Run test_Orchestrator test")
        # We reset cfg.my_device_id to override the computers mac_address
        # This is a test device defined to have a DummySensor.
        root_cfg.update_my_device_id("d01111111111")

        with rpi_emulator.RpiEmulator.get_instance() as th:
            logger.debug("sensor_test: # Test orchestrator")
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(INVENTORY)

            sc = RpiCore()
            sc.configure(inventory)

            orchestrator = EdgeOrchestrator.get_instance()
            orchestrator.load_config()
            orchestrator.start_all()
            sleep(10)
            orchestrator.stop_all()
            sleep(2)
            # Check that we have data in the journals
            # SCORE & SCORP & DUMML & DUMMF should contain data.
            # DUMMD should be empty
            # The files will have been pushed to the cloud, so we need to get
            # the modified data on each journal.
            th.assert_records("expidite-journals",
                            {"V3_DUMML*": 1, "V3_DUMMD*": 1})
            th.assert_records("expidite-upload",
                            {"V3_DUMMF*": th.ONE_OR_MORE})
            th.assert_records("expidite-system-records",
                            {"V3_SCORE*": 1, "V3_SCORP*": 1})
            th.assert_records("expidite-fair",
                            {"V3_*": 1})

            # Stop without start
            orchestrator.load_config()
            sleep(1)
            orchestrator.stop_all()

            # Repeat runs of observability logging
            logger.info("sensor_test: # Repeat runs of observability logging")
            orchestrator.load_config()
            orchestrator.start_all()
            orchestrator.stop_all()


    def test_orchestrator_main(self) -> None:
        # We reset cfg.my_device_id to override the computers mac_address
        # This is a test device defined to have a DummySensor.
        logger.info("Run test_orchestrator_main test")
        root_cfg.update_my_device_id("d01111111111")

        with rpi_emulator.RpiEmulator.get_instance():
            orchestrator = EdgeOrchestrator.get_instance()
            inventory = root_cfg.load_configuration()
            if inventory:
                root_cfg.set_inventory(inventory)

            # Direct use of edge_orchestrator to include main() keep-alive
            logger.info("sensor_test: Direct use of EdgeOrchestor to include keep-alive")
            factory_thread = Thread(target=edge_orchestrator.main)
            factory_thread.start()
            start_clock = api.utc_now()
            while not orchestrator.watchdog_file_alive():
                sleep(1)
                assert (api.utc_now() - start_clock).total_seconds() < 10, (
                    "Orchestrator did not restart quickly enough")
            assert orchestrator.watchdog_file_alive()

            # Sensor fails; factory_thread should restart everything after 1s
            logger.info("sensor_test: # Sensor fails; factory_thread should restart everything after 1s")
            sensor = orchestrator._get_sensor(api.SENSOR_TYPE.I2C, 1)
            assert sensor is not None

            sensor.sensor_failed()
            assert root_cfg.RESTART_EXPIDITE_FLAG.exists()
            start_clock = api.utc_now()
            while root_cfg.RESTART_EXPIDITE_FLAG.exists():
                sleep(1)
                assert (api.utc_now() - start_clock).total_seconds() < 10, (
                    "Orchestrator did not restart quickly enough")
            sleep(3)
            orchestrator.stop_all()

            # Wait for the main thread to exit
            logger.info("sensor_test: # Stop the edge_orchestrator main loop")
            if factory_thread.is_alive():
                factory_thread.join()
