import logging
from threading import Thread
from time import sleep

import pytest
from example.my_fleet_config import INVENTORY
from sensor_core import api, edge_orchestrator
from sensor_core import configuration as root_cfg
from sensor_core.edge_orchestrator import EdgeOrchestrator
from sensor_core.sensor_core import SensorCore
from sensor_core.utils import sc_test_emulator

logger = root_cfg.setup_logger("sensor_core", level=logging.DEBUG)

root_cfg.TEST_MODE = root_cfg.MODE.TEST

class Test_Orchestrator:
    @pytest.mark.quick
    def test_SensorCore_status(self) -> None:
        sc = SensorCore()
        sc.configure(INVENTORY)
        message = sc.status()
        logger.info(message)
        assert message is not None


    @pytest.mark.quick
    def test_Orchestrator(self) -> None:
        with sc_test_emulator.ScEmulator.get_instance() as th:
            # Mock the timers in the inventory for faster testing
            inventory = th.mock_timers(INVENTORY)

            logger.debug("sensor_test: # Test orchestrator")
            # Standard flow
            # We reset cfg.my_device_id to override the computers mac_address
            # This is a test device defined in BeeOps.cfg to have a DummySensor.
            root_cfg.update_my_device_id("d01111111111")

            sc = SensorCore()
            sc.configure(inventory)

            orchestrator = EdgeOrchestrator.get_instance()
            orchestrator.load_config()
            orchestrator.start_all()
            sleep(12)
            orchestrator.stop_all()
            sleep(2)
            # Check that we have data in the journals
            # SCORE & SCORP & DUMML & DUMMF should contain data.
            # DUMMD should be empty
            # The files will have been pushed to the cloud, so we need to get 
            # the modified data on each journal.
            th.assert_records("sensor-core-journals",
                            {"V3_DUMML*": 1, "V3_DUMMD*": 1})
            th.assert_records("sensor-core-upload",
                            {"V3_DUMMF*": th.ONE_OR_MORE})
            th.assert_records("sensor-core-system-records",
                            {"V3_SCORE*": 1, "V3_SCORP*": 1})
            th.assert_records("sensor-core-fair",
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
        
        with sc_test_emulator.ScEmulator.get_instance():

            orchestrator = EdgeOrchestrator.get_instance()
            root_cfg.set_inventory(root_cfg.load_configuration())

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
            assert root_cfg.RESTART_SENSOR_CORE_FLAG.exists()
            start_clock = api.utc_now()
            while root_cfg.RESTART_SENSOR_CORE_FLAG.exists():
                sleep(1)
                assert (api.utc_now() - start_clock).total_seconds() < 10, (
                    "Orchestrator did not restart quickly enough")
            sleep(3)
            orchestrator.stop_all()

            # Wait for the main thread to exit
            logger.info("sensor_test: # Stop the edge_orchestrator main loop")
            if factory_thread.is_alive():
                factory_thread.join()
