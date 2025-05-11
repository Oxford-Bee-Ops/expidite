####################################################################################################
# EdgeOrchestrator: Manages the state of the sensor threads
####################################################################################################
import threading
from datetime import timedelta
from enum import Enum
from time import sleep
from typing import Callable, Optional

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import AsyncCloudConnector, CloudConnector
from sensor_core.device_health import DeviceHealth
from sensor_core.device_manager import DeviceManager
from sensor_core.dp_node import DPnode
from sensor_core.dp_tree import DPtree
from sensor_core.dp_worker_thread import DPworker
from sensor_core.sensor import Sensor
from sensor_core.stats_tracker import StatTracker
from sensor_core.utils.journal_pool import JournalPool

logger = root_cfg.setup_logger("sensor_core")

class OrchestratorStatus(Enum):
    """Enum for the status of the orchestrator"""
    STOPPED = 0
    STARTING = 1
    RUNNING = 2
    STOPPING = 3

    def __str__(self) -> str:
        return self.name.lower()
    
    @staticmethod
    def running(status: "OrchestratorStatus") -> bool:
        """Check if the orchestrator is starting"""
        return (status == OrchestratorStatus.STARTING or
                status == OrchestratorStatus.RUNNING)

    def stopped(status: "OrchestratorStatus") -> bool:
        """Check if the orchestrator is stopped"""
        return (status == OrchestratorStatus.STOPPED or
                status == OrchestratorStatus.STOPPING)

class EdgeOrchestrator:
    """The EdgeOrchestrator manages the state of the sensors and their associated Datastreams.

    Started by the SensorFactory, which creates the sensors and registers them with the EdgeOrchestrator.
    The EdgeOrchestrator:
    - interrogates the Sensor to get its Datastreams
    - starts the Sensor and Datastream threads
    - starts an observability thread to monitor the performance of the SensorCore
    """

    _instance = None
    _status_lock = threading.RLock()  # Re-entrant lock to ensure thread-safety

    root_cfg.set_mode(root_cfg.Mode.EDGE)

    def __new__(cls, *args, **kwargs): # type: ignore
        if not cls._instance:
            cls._instance = super(EdgeOrchestrator, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        logger.info(f"Initialising EdgeOrchestrator {self!r}")

        self._status = OrchestratorStatus.STOPPED
        self.reset_orchestrator_state()

        logger.info(f"Initialised EdgeOrchestrator {self!r}")

    @staticmethod
    def get_instance() -> "EdgeOrchestrator":
        """Get the singleton instance of the EdgeOrchestrator"""
        with EdgeOrchestrator._status_lock:
            if EdgeOrchestrator._instance is None:
                EdgeOrchestrator._instance = EdgeOrchestrator()

        return EdgeOrchestrator._instance

    def reset_orchestrator_state(self) -> None:
        logger.debug("Reset orchestrator state")

        with EdgeOrchestrator._status_lock:
            self._sensorThreads: list[Sensor] = []
            self._dpworkers: list[DPworker] = []
            self.dp_trees: list[DPtree] = []
            
            # We create a series of special Datastreams for recording:
            # HEART - device health
            # WARNING - captures error & warning logs
            # SCORE - data save events
            # SCORP - DP performance
            self.device_health = DeviceHealth()
            self.device_manager: DeviceManager | None = None
            health_dpe = DPworker(DPtree(self.device_health))
            self._sensorThreads.append(self.device_health)
            self._dpworkers.append(health_dpe)

            self.selftracker = StatTracker()
            tracker_dpe = DPworker(DPtree(self.selftracker))
            self._sensorThreads.append(self.selftracker)
            self._dpworkers.append(tracker_dpe)
            self.selftracker.set_dpworkers(self._dpworkers)
            # We set the _selftracker as a class variable so that all DPtreeNoes instances can 
            # log their performance data
            DPnode._selftracker = self.selftracker



    def status(self) -> dict[str, str]:
        """Return a key-value status describing the state of the EdgeOrchestrator"""
        # Check that all threads are alive
        sensors_alive = 0
        for sensor in self._sensorThreads:
            if not sensor.is_alive():
                logger.warning(f"Sensor thread {sensor} is not alive")
            else:
                sensors_alive += 1
        dps_alive = 0
        for dpe in self._dpworkers:
            if not dpe.is_alive():
                logger.debug(f"Datastream thread {dpe} is not alive")
            else:
                dps_alive += 1

        status = {
            "SensorCore running": str(self.watchdog_file_alive()),
            "Sensor threads": str(self._sensorThreads),
            "Sensor threads alive": str(sensors_alive),
            "DPtrees": str(self._dpworkers),
            "DPtrees alive": str(dps_alive),
        }
        return status

    def load_config(self) -> None:
        """Load the sensor and data processor config into the EdgeOrchestrator by calling
        the DeviceCfg.dp_trees_create_method()."""
        self.dp_trees = self._safe_call_create_method(root_cfg.my_device.dp_trees_create_method)
        for dptree in self.dp_trees:
            sensor = dptree.sensor
            if sensor in self._sensorThreads:
                logger.error(f"{root_cfg.RAISE_WARN()}Sensor already added: {sensor!r}")
                logger.info(self.status())
                raise ValueError(f"Sensor already added: {sensor!r}")
            self._sensorThreads.append(sensor)
            self._dpworkers.append(DPworker(dptree))

    @staticmethod
    def _safe_call_create_method(create_method: Optional[Callable]) -> list[DPtree]:
        """Call the create method and return the DPtree object.
        Raises ValueError if the create method does not successfully create any DPtree objects."""
        if create_method is None:
            logger.error(f"{root_cfg.RAISE_WARN()}create_method not defined for {root_cfg.my_device_id}")
            raise ValueError(f"create_method not defined for {root_cfg.my_device_id}")

        logger.info(f"Creating DP trees for {root_cfg.my_device_id} using {create_method}")
        dp_trees: list[DPtree] = create_method()

        if not dp_trees:
            logger.error(f"{root_cfg.RAISE_WARN()}No sensors created by {root_cfg.my_device_id} "
                            f"{create_method}")
            raise ValueError(f"No sensors created by {create_method}")

        if not isinstance(dp_trees, list):
            logger.error(f"{root_cfg.RAISE_WARN()}create_method must return a list; "
                         f"created {dp_trees.__type__}")
            raise ValueError("create_method must return a list of DPtree objects")
        
        return dp_trees

    #########################################################################################################
    #
    # Sensor interface
    #
    #########################################################################################################
    def sensor_failed(self, sensor: Sensor) -> None:
        """Called by Sensor to indicate that it has failed; orchestrator will then restarting everything."""
        logger.error(f"{root_cfg.RAISE_WARN()}Sensor failed; restarting all; {sensor}")
        logger.info(self.status())

        # We must *not* call stop_all() here, as that will cause a deadlock because we're currently in the 
        # sensor thread that stop_all will wait on.
        # Instead, we set the RESTART flag, and the main() method will check for them.
        root_cfg.RESTART_SENSOR_CORE_FLAG.touch()


    def _get_sensor(self, sensor_type: api.SENSOR_TYPE, sensor_index: int) -> Optional[Sensor | None]:
        """Private method to get a sensor by type & index"""
        logger.debug(f"_get_sensor {sensor_type} {sensor_index} from {self._sensorThreads}")
        for sensor in self._sensorThreads:
            if (sensor.config.sensor_type == sensor_type) and (sensor.sensor_index == sensor_index):
                return sensor
        return None


    #########################################################################################################
    #
    # Management of Sensor and Datastream threads
    #
    #########################################################################################################
    def start_all(self) -> None:
        """Start all Sensor & DPworker threads"""

        with EdgeOrchestrator._status_lock:

            if self._status != OrchestratorStatus.STOPPED:
                logger.warning(f"EdgeOrchestrator is already running; {self}; {self._status}")
                logger.info(self.status())
                return
            
            self._status = OrchestratorStatus.STARTING
            
            # Check the "stop" file has been cleared
            root_cfg.STOP_SENSOR_CORE_FLAG.unlink(missing_ok=True)
            root_cfg.RESTART_SENSOR_CORE_FLAG.unlink(missing_ok=True)

            logger.info(f"Starting EdgeOrchestrator {self!r}")


        # Start the device manager if we're on RPi
        if root_cfg.running_on_rpi:
            self.device_manager = DeviceManager()

        # Start the DPworker threads
        for dpe in self._dpworkers:
            dpe.start()

        # Only once we've started the datastreams, do we start the Sensor threads
        # otherwise we get a "Datastream not started" error.
        for sensor in self._sensorThreads:
            sensor.start()

        # Dump status to log
        logger.info(f"EdgeOrchestrator started: {self.status()}")

        with EdgeOrchestrator._status_lock:
            self._status = OrchestratorStatus.RUNNING


    @staticmethod
    def start_all_with_watchdog() -> None:
        """This function starts the orchestrator and maintains it with a watchdog.
        This is a non-blocking function that starts a new thread and returns.
        It calls the edge_orchestrator main() function."""

        logger.debug("Start orchestrator with watchdog")
        # @@@ We should do this as a separate process, so we can exit without killing SC
        # or without hanging on exit.
        orchestrator_thread = threading.Thread(target=main, name="EdgeOrchestrator")
        orchestrator_thread.start()
        # Block for long enough for the main thread to be scheduled
        # So we avoid race conditions with subsequence calls to stop_all()
        sleep(1)


    def stop_all(self, restart: Optional[bool] = False) -> None:
        """Stop all Sensor, Datastream and observability threads

        Blocks until all threads have exited"""

        logger.info(f"stop_all on {self!r} called by {threading.current_thread().name}")

        with EdgeOrchestrator._status_lock:
            if not self._status == OrchestratorStatus.RUNNING:
                logger.warning(f"EdgeOrchestrator not running when stop called; {self}")
                logger.info(self.status())
                self.reset_orchestrator_state()
                return

            self._status = OrchestratorStatus.STOPPING

            # Set the STOP_SENSOR_CORE_FLAG file; this is polled by the main() method in 
            # the EdgeOrchestrator which will continue to restart the SensorCore until the flag is removed.
            # This is also important when we are not the running instance of the orchestrator,
            # as the running instance will check the file and stop itself.
            if not restart:
                root_cfg.STOP_SENSOR_CORE_FLAG.touch()
                root_cfg.RESTART_SENSOR_CORE_FLAG.unlink(missing_ok=True)
            else:
                # We use stop_all to restart the orchestrator cleanly in the event of a sensor failure.
                logger.info("Restart requested; clearing stop & restart flags")
                root_cfg.STOP_SENSOR_CORE_FLAG.unlink(missing_ok=True)
                root_cfg.RESTART_SENSOR_CORE_FLAG.unlink(missing_ok=True)

        # Stop the device manager if we're on RPi
        if self.device_manager is not None:
            self.device_manager.stop()
            self.device_manager = None

        # Stop all the sensor threads
        for sensor in self._sensorThreads:
            sensor.stop()

        # Block until all Sensor threads have exited
        for sensor in self._sensorThreads:
            # We need the check that the thread we're waiting on is not our own thread,
            # because that will cause a RuntimeError
            our_thread = threading.current_thread().ident
            if (sensor.ident != our_thread) and sensor.is_alive():
                logger.info(f"Waiting for sensor thread {sensor}")
                sensor.join()
                logger.info(f"Waiting over. Sensor thread {sensor} stopped")
            else:
                logger.info(f"Sensor thread {sensor} already stopped")

        # Stop all the dataprocessor threads
        for dpe in self._dpworkers:
            dpe.stop()

        # Block until all Datastreams have exited
        for dpe in self._dpworkers:
            if dpe.is_alive():
                logger.info(f"Waiting for datastream thread {dpe}")
                dpe.join()
                logger.info(f"Waiting over. Datastream thread {dpe} stopped")
            else:
                logger.info(f"Datastream thread {dpe} already stopped")

        # Trigger a flush_all on the CloudJournals so we save collected information 
        # before we kill everything
        jp = JournalPool.get(root_cfg.Mode.EDGE)
        jp.flush_journals()
        jp.stop()
        # jp.stop will also stop the cloud connector threadpool
        # Clean up and exit
        cc = CloudConnector.get_instance(type=root_cfg.CloudType.AZURE)
        assert isinstance(cc, AsyncCloudConnector)
        cc.shutdown()

        # Clear our thread lists
        self.reset_orchestrator_state()

        with EdgeOrchestrator._status_lock:
            self._status = OrchestratorStatus.STOPPED
            logger.info("Stopped all sensors and datastreams")


    def is_stop_requested(self) -> bool:
        """Check if a stop has been manually requested by the user.
        This function is polled by the main thread every second to check if the user has requested a stop."""
        return root_cfg.STOP_SENSOR_CORE_FLAG.exists()


    @staticmethod
    def watchdog_file_alive() -> bool:
        """Check if the SensorCore is running"""
        # If the SENSOR_CORE_IS_RUNNING_FLAG exists and was touched within the last 2x _FREQUENCY seconds,
        # and the timestamp on the file is < than the timestamp on the STOP_SENSOR_CORE_FLAG file,
        # then we are running.
        # If the file doesn't exist, we are not running.
        # If the file exists, but was not touched within the last 2x _FREQUENCY seconds, we are not running.

        if not root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.exists():
            return False
        
        if (root_cfg.STOP_SENSOR_CORE_FLAG.exists() and 
            (root_cfg.STOP_SENSOR_CORE_FLAG.stat().st_mtime >
             root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.stat().st_mtime)):
                return False
        
        time_threshold = api.utc_now() - timedelta(seconds=2 * root_cfg.WATCHDOG_FREQUENCY)
        if root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.stat().st_mtime < time_threshold.timestamp():
            return False
        
        # If we get here, the file exists, was touched within the last 2x _FREQUENCY seconds,
        # and the timestamp is > than the timestamp on the STOP_SENSOR_CORE_FLAG file.
        return True

#############################################################################################################
# Orchestrator main loop
#
# Main loop called from crontab on boot up
#############################################################################################################
def _touch_watchdog_file() -> None:
    """Touch the running file to indicate that the script is running"""
    root_cfg.SENSOR_CORE_IS_RUNNING_FLAG.touch()


def main() -> None:
    try:
        # Provide diagnostics
        logger.info(root_cfg.my_device.display())

        orchestrator = EdgeOrchestrator.get_instance()
        if (orchestrator.watchdog_file_alive() or 
            OrchestratorStatus.running(orchestrator._status)):
            logger.warning("SensorCore is already running; exiting")
            return

        orchestrator.load_config()

        # Start all the sensor threads
        orchestrator.start_all()

        # Keep the main thread alive
        while not orchestrator.is_stop_requested():
            if root_cfg.RESTART_SENSOR_CORE_FLAG.exists():
                # Restart the re-load and re-start the EdgeOrchestrator if it fails.
                logger.error(f"Orchestrator failed; restarting; {orchestrator._status}")
                orchestrator.stop_all()
                orchestrator.load_config()
                orchestrator.start_all()
            else:   
                logger.debug(f"Orchestrator running ({orchestrator._status})")
                _touch_watchdog_file()

            sleep(root_cfg.WATCHDOG_FREQUENCY)


    except Exception as e:
        logger.error(
            f"{root_cfg.RAISE_WARN()}(Sensor exception: {e!s}",
            exc_info=True,
        )
    finally:
        # To get here, we hit an exception on one thread or have been explicitly asked to stop.
        # Tell all threads to terminate so we can cleanly restart all via cron
        if orchestrator is not None:
            logger.info("Edge orchestrator exiting; stopping all sensors and datastreams")
            orchestrator.stop_all()
        logger.info("Sensor script finished")


#############################################################################################################
# Main
#
# Use cfg to determine which sensors are installed on this device, and start the appropriate threads
#############################################################################################################
# Main loop called from crontab on boot up
if __name__ == "__main__":
    print("Starting EdgeOrchestrator")
    main()
