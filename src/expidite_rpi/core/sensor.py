##############################################################################################################
# Sensor classes
# - EdgeOrchestrator: Manages the state of the sensor threads
# - SensorConfig: Dataclass for sensor configuration, specified in sensor_cac.py
# - Sensor: Super class for all sensor classes
##############################################################################################################
import subprocess
from abc import ABC
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import SensorCfg, SensorMode
from expidite_rpi.core.dp_node import DPnode
from expidite_rpi.core.hardware.button import ButtonInput
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")


##############################################################################################################
# Super class that implements a thread to read the sensor data
##############################################################################################################
class Sensor(Thread, DPnode, ABC):
    # Create a class variable to track the review_mode status
    review_mode: bool = False
    last_checked_review_mode: datetime = datetime.min.replace(tzinfo=UTC)
    review_mode_check_interval: timedelta = timedelta(seconds=5)

    def __init__(self, config: SensorCfg) -> None:
        """Initialise the Sensor superclass.

        Parameters:
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

        # The long-running subprocess (if any) currently executing on this sensor's thread, so that stop()
        # can abort it. Guarded by a lock because stop() runs on the orchestrator thread while
        # set_active_subprocess() runs on this sensor's thread. See set_active_subprocess().
        self._active_subprocess: subprocess.Popen[bytes] | None = None
        self._active_subprocess_lock = Lock()

    def start(self) -> None:
        """Start the sensor thread - this method must not be subclassed."""
        logger.info(f"Starting sensor thread {self!r}")
        super().start()

    def set_active_subprocess(self, p: subprocess.Popen[bytes]) -> None:
        """Record the subprocess currently running on this sensor's thread so stop() can abort it.

        Sensors that shell out to a long-running command (e.g. a video recording) pass this method as
        utils.run_cmd's on_start callback. It lets a shutdown kill the command immediately instead of the
        sensor thread blocking - and hence delaying RpiCore shutdown - until the command finishes on its own.
        """
        with self._active_subprocess_lock:
            self._active_subprocess = p

    def stop(self) -> None:
        """Stop the sensor thread - this method must not be subclassed."""
        logger.info(f"Stop sensor thread {self!r}")
        self.stop_requested.set()
        # If a long-running command (e.g. a video recording) is in progress on this sensor's thread, kill it
        # so the thread returns promptly rather than blocking shutdown until the command's own timer expires.
        # The poll() guard means we never signal a process that has already exited (whose pid the OS may have
        # reused); once it has exited its returncode is set, so a genuine failure is still surfaced normally.
        with self._active_subprocess_lock:
            p = self._active_subprocess
            if p is not None and p.poll() is None:
                logger.info(f"Aborting in-progress command on sensor {self!r}")
                utils.kill_process_group(p)

    def discard_if_stopping(self, partial_file: Path) -> bool:
        """If shutdown has been requested, discard a partial recording and return True.

        A recording that stop() aborted mid-flight (see set_active_subprocess) leaves a partial, shorter-
        than-usual file. Rather than saving that, sensors call this immediately after the recording command
        returns: if we are stopping it deletes the file and returns True so the caller can break/return out
        of its recording loop; otherwise it returns False and the caller saves the recording as normal.
        """
        if not self.stop_requested.is_set():
            return False
        partial_file.unlink(missing_ok=True)
        logger.info("Recording aborted by shutdown; discarding partial output")
        return True

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

        return not self.stop_requested.is_set()

    def reducing_load_advised(self) -> bool:
        """Sensor subclasses can call this function to check if they should reduce their load.

        This is advisory only and does not hold up the thread like continue_recording, but it can be used
        by sensors to decide, for example, to skip sensing cycles or reduce frequency or resolution.
        """
        return utils.reduce_load_advised()

    def in_review_mode(self) -> bool:
        """Returns true if the system is in review mode.

        The intent of review mode is to help with manual review of sensor data.
        The actual implementation is up to the Sensor subclass.
        But, for example, the video sensor saves images to the cloud every few seconds with the same
        name so that the user positioning a camera can see what the camera is seeing in near-real time.
        I2C sensors will typically increase their logging frequency to help with manual review.

        Review mode is set via the BCLI. The BCLI also helps the user understand how to see the
        output from the sensors in review mode.

        Review mode is indicated by the presence of the REVIEW_MODE_FLAG file.
        """
        # We maintain a class variable to avoid repeated filesystem checks.
        if (datetime.now(tz=UTC) - Sensor.last_checked_review_mode) > Sensor.review_mode_check_interval:
            Sensor.last_checked_review_mode = datetime.now(tz=UTC)
            Sensor.review_mode = root_cfg.REVIEW_MODE_FLAG.exists()
            if Sensor.review_mode:
                # Check the timestamp of the flag file to ensure it is sufficiently recent.
                # We auto exit review mode if the flag file is stale (>24 hours).
                flag_age = datetime.now(tz=UTC) - datetime.fromtimestamp(
                    root_cfg.REVIEW_MODE_FLAG.stat().st_mtime, tz=UTC
                )
                if flag_age > timedelta(hours=24):
                    Sensor.review_mode = False
                    root_cfg.REVIEW_MODE_FLAG.unlink(missing_ok=True)
                    logger.info("Review mode flag file is stale; cleaning up")
        return Sensor.review_mode

    def sensor_failed(self) -> None:
        """Called by a subclass when the Sensor fails and needs to be restarted.

        The Sensor superclass notifies the EdgeOrchestrator which will stop & restart all Sensors.
        """
        from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator

        EdgeOrchestrator.get_instance().sensor_failed(self)

    # Sensors should sub-class this method to implement continuous sensing.
    # If not sub-classed, this default implements on-demand triggered sensing.
    # Implementations should use continue_recording() to control recording loops and terminate within a
    # reasonable time (~3min).
    def run(self) -> None:
        """The run method is where the sensor does its work of sensing and logging data.
        For continuous sensing, this method should be sub-classed to implement the sensing loop.
        The subclass *must* use "while self.continue_recording()" and
        "self.stop_requested.wait(sleep_time)" to enable prompt and clean shutdown.
        For on-demand, triggered sensing, the subclass may leave this method unimplemented, but
        instead implement the sensing_triggered() method which will be invoked when triggered.
        """
        logger.info(f"Sensor {self!r} running in on-demand triggered sensing mode")
        if self.config.sensor_mode == SensorMode.BCLI_TRIGGERED:
            self._run_bcli_triggered(duration=30)
        elif self.config.sensor_mode == SensorMode.BUTTON_TRIGGERED:
            self._run_button_triggered()

    def _run_bcli_triggered(self, duration: int) -> None:
        """This method is invoked by the BCLI when a sensing trigger is detected. The default implementation
        simply calls sensing_triggered, but the run can be sub-classed to implement custom behavior
        on BCLI triggers.
        """
        logger.info(f"Sensor {self!r} running in on-demand triggered sensing mode")
        while self.continue_recording():
            # Check for the sensing trigger set via the BCLI
            if root_cfg.SENSOR_TRIGGER_FLAG.exists():
                try:
                    start_time = datetime.now(tz=UTC)
                    # Read the duration from the flag file
                    with open(root_cfg.SENSOR_TRIGGER_FLAG) as f:
                        duration_str = f.read().strip()
                    duration = int(duration_str)

                    # Invoke the sensing_triggered method
                    logger.info(f"Sensing trigger detected for duration {duration} seconds")
                    self.sensing_triggered(duration)

                    # Clear the file after processing and after the requisite duration
                    elapsed = (datetime.now(tz=UTC) - start_time).total_seconds()
                    if elapsed < duration:
                        wait_time = duration - elapsed
                        logger.info(f"Waiting additional {wait_time:.1f}s to complete requested duration")
                        self.stop_requested.wait(wait_time)
                    root_cfg.SENSOR_TRIGGER_FLAG.unlink(missing_ok=True)

                except Exception:
                    logger.exception("Error processing sensing trigger")
                    # Ensure the trigger flag file is removed on error
                    root_cfg.SENSOR_TRIGGER_FLAG.unlink(missing_ok=True)
            self.stop_requested.wait(1)

    def _run_button_triggered(self) -> None:
        """This method is invoked when a button trigger is detected. The default implementation simply calls
        sensing_triggered with a default duration, but this method can be sub-classed to implement custom
        behavior on button triggers.
        """
        pin = self.config.button_gpio_pin if self.config.button_gpio_pin is not None else 27
        button = ButtonInput(pin=pin)
        try:
            while self.continue_recording():
                # Wait_for_press returns every so often to enable checking the continue_recording condition
                # and clean shutdown. If this is the case, rc will be false and we simply loop back and wait
                # again.
                rc = button.wait_for_press()
                if rc:
                    self.sensing_triggered(duration=0)
                logger.info("Button pressed -> trigger sequence")
        except Exception:
            logger.exception("run_button_triggered encountered an error")
        finally:
            button.cleanup()
            logger.info("Button input cleaned up")

    # Sensors should sub-class this method to implement on-demand, triggered, sensing.
    def sensing_triggered(self, duration: int) -> None:
        """The sensing_triggered method is where the sensor does its work of sensing and logging data
        in response to an external trigger, typically being invoked via the BCLI.
        """
        msg = "Sub-classes must override this method"
        raise AssertionError(msg)
