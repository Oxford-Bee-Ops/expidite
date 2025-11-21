####################################################################################################
# Sensor classes
#  - EdgeOrchestrator: Manages the state of the sensor threads
#  - SensorConfig: Dataclass for sensor configuration, specified in sensor_cac.py
#  - Sensor: Super class for all sensor classes
####################################################################################################
from abc import ABC
from datetime import datetime, timedelta
from threading import Event, Thread

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import SensorCfg
from expidite_rpi.core.dp_node import DPnode
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")


#############################################################################################################
# Super class that implements a thread to read the sensor data
#############################################################################################################
class Sensor(Thread, DPnode, ABC):
    # Create a class variable to track the review_mode status
    review_mode: bool = False
    last_checked_review_mode: datetime = datetime.min
    review_mode_check_interval: timedelta = timedelta(seconds=5)

    def __init__(self, config: SensorCfg) -> None:
        """Initialise the Sensor superclass.

        Parameters:
        ----------
        sensor_index: int
            The index of the sensor in the list of sensors.
        sensor_config: SensorConfig
            The configuration for the sensor.
        """
        Thread.__init__(self, name=self.__class__.__name__)
        DPnode.__init__(self, config, config.sensor_index)

        logger.info(f"Initialise sensor {self!r}")

        self.config = config

        # We set the daemon status to true so that the thread continues to run in the background
        self.daemon = False
        self.stop_requested = Event()

    def start(self) -> None:
        """Start the sensor thread - this method must not be subclassed"""
        logger.info(f"Starting sensor thread {self!r}")
        super().start()

    def stop(self) -> None:
        """Stop the sensor thread - this method must not be subclassed"""
        logger.info(f"Stop sensor thread {self!r}")
        self.stop_requested.set()

    def continue_recording(self) -> bool:
        """Sensor subclasses *must* use this in a while loop to manage the recording cycle.

        This method is called by the Sensor subclass to check if it should continue recording.
        If Expidite is shutting down, this will return false.
        If Expidite is failing to process data quickly enough and is therefore at risk of
        running out of memory, this function will hold up the thread until the data backlog is
        processed.
        """
        # Check if the system is running low on memory
        while utils.failing_to_keep_up():
            self.stop_requested.wait(root_cfg.my_device.max_recording_timer)

        if self.stop_requested.is_set():
            return False
        else:
            return True

    def in_review_mode(self) -> bool:
        """Returns true if the system is in review mode.

        The intent of review mode is to help with manual review of sensor data.
        The actual implementation is up to the Sensor subclass.
        But, for example, the video sensor saves images to the cloud every few seconds with the same
        name so that the user positioning a camera can see what the camera is seeing in near-real time.
        I2C sensors will typically increase their logging frequency to help with manual review.

        Review mode is set via the BCLI.  The BCLI also helps the user understand how to see the
        output from the sensors in review mode.

        Review mode is indicated by the presence of the REVIEW_MODE_FLAG file.
        """
        # We maintain a class variable to avoid repeated filesystem checks.
        if (datetime.now() - Sensor.last_checked_review_mode) > Sensor.review_mode_check_interval:
            Sensor.last_checked_review_mode = datetime.now()
            Sensor.review_mode = root_cfg.REVIEW_MODE_FLAG.exists()
            if Sensor.review_mode:
                # Check the timestamp of the flag file to ensure it is sufficiently recent
                # We auto exit review mode if the flag file is stale (>30 mins)
                flag_age = datetime.now() - datetime.fromtimestamp(root_cfg.REVIEW_MODE_FLAG.stat().st_mtime)
                if flag_age > timedelta(minutes=30):
                    Sensor.review_mode = False
                    root_cfg.REVIEW_MODE_FLAG.unlink(missing_ok=True)
                    logger.info("Review mode flag file is stale; cleaning up")
        return Sensor.review_mode

    def sensor_failed(self) -> None:
        """Called by a subclass when the Sensor fails and needs to be restarted.

        The Sensor superclass notifies the EdgeOrchestrator which will stop & restart all Sensors."""
        from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator

        EdgeOrchestrator.get_instance().sensor_failed(self)

    # Sensors should sub-class this method to implement continuous sensing.
    # If not sub-classed, this default implements on-demand triggered sensing.
    # Implementations should use continue_recording() to control recording loops and terminate
    # within a reasonable time (~3min).
    def run(self) -> None:
        """The run method is where the sensor does its work of sensing and logging data.
        For continuous sensing, this method should be sub-classed to implement the sensing loop.
        The subclass *must* use "while self.continue_recording()" and
        "self.stop_requested.wait(sleep_time)" to enable prompt and clean shutdown.
        For on-demand, triggered sensing, the subclass may leave this method unimplemented, but
        instead implement the sensing_triggered() method which will be invoked when triggered."""
        logger.info(f"Sensor {self!r} running in on-demand triggered sensing mode")
        while self.continue_recording():
            # Check for the sensing trigger set via the BCLI
            if root_cfg.SENSOR_TRIGGER_FLAG.exists():
                try:
                    start_time = datetime.now()
                    # Read the duration from the flag file
                    with open(root_cfg.SENSOR_TRIGGER_FLAG, "r") as f:
                        duration_str = f.read().strip()
                    duration = int(duration_str)

                    # Invoke the sensing_triggered method
                    logger.info(f"Sensing trigger detected for duration {duration} seconds")
                    self.sensing_triggered(duration)

                    # Clear the file after processing and after the requisite duration
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed < duration:
                        wait_time = duration - elapsed
                        logger.info(f"Waiting additional {wait_time:.1f}s to complete requested duration")
                        self.stop_requested.wait(wait_time)
                    root_cfg.SENSOR_TRIGGER_FLAG.unlink(missing_ok=True)

                except Exception as e:
                    logger.error(f"Error processing sensing trigger: {e}", exc_info=True)
                    # Ensure the trigger flag file is removed on error
                    root_cfg.SENSOR_TRIGGER_FLAG.unlink(missing_ok=True)
            self.stop_requested.wait(1)


    # Sensors should sub-class this method to implement on-demand, triggered, sensing.
    def sensing_triggered(self, duration: int) -> None:
        """The sensing_triggered method is where the sensor does its work of sensing and logging data
        in response to an external trigger, typically being invoked via the BCLI."""

        assert False, "Sub-classes must override this method"
