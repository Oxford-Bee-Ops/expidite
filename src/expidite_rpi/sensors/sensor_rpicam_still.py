####################################################################################################
# Sensor class that provides a direct map onto Raspberry Pi's rpicam-still.
#
# The user specifies the rpicam-still command line, except for the file name, which is set by RpiCore.
#
####################################################################################################

from dataclasses import dataclass

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

RPICAM_STILL_DATA_TYPE_ID = "RPICAM"
RPICAM_STILL_STREAM_INDEX: int = 0

@dataclass
class RpicamStillSensorCfg(SensorCfg):
    ############################################################
    # Add custom fields
    ############################################################
    # Defines the rpicam-still command to use to record video.
    # This should be as specified in the rpicam-still documentation.
    # The filename should be substituted with FILENAME. 
    # The FILENAME suffix should match the datastream input_format.
    rpicam_cmd: str = "rpicam-still -o FILENAME"
    recording_interval_seconds: int = 60*60  # Interval between still images

DEFAULT_RPICAM_STILL_SENSOR_CFG = RpicamStillSensorCfg(
    sensor_type=api.SENSOR_TYPE.CAMERA,
    sensor_index=0,
    sensor_model="PiCameraModule3",
    description="Video sensor that uses rpicam-still",
    outputs=[
        Stream(
            description="Basic still image recorder.",
            type_id=RPICAM_STILL_DATA_TYPE_ID,
            index=RPICAM_STILL_STREAM_INDEX,
            format=api.FORMAT.JPG,
            cloud_container="expidite-upload",
            sample_probability="1.0",
        )
    ],
    rpicam_cmd = "rpicam-still -o FILENAME",
    recording_interval_seconds = 60*60,  # Interval between still images
)

class RpicamStillSensor(Sensor):
    def __init__(self, config: RpicamStillSensorCfg):
        """Constructor for the RpicamStillSensor class"""
        super().__init__(config)
        self.config = config
        self.recording_format = self.get_stream(RPICAM_STILL_STREAM_INDEX).format
        self.rpicam_cmd = self.config.rpicam_cmd
        self.recording_interval_seconds = config.recording_interval_seconds

        assert self.rpicam_cmd, (
            f"rpicam_cmd must be set in the sensor configuration: {self.rpicam_cmd}"
        )
        assert self.rpicam_cmd.startswith("rpicam-still "), (
            f"rpicam_cmd must start with 'rpicam-still ': {self.rpicam_cmd}"
        )
        assert "FILENAME" in self.rpicam_cmd, (
            f"FILENAME placeholder missing in rpicam_cmd: {self.rpicam_cmd}"
        )
        assert "FILENAME " in self.rpicam_cmd, (
            f"FILENAME placeholder should be specified without any suffix rpicam_cmd: {self.rpicam_cmd}"
        )


    def run(self):
        """Main loop for the RpicamStillSensor - runs continuously unless paused."""
        if not root_cfg.running_on_rpi and root_cfg.TEST_MODE != root_cfg.MODE.TEST:
            logger.warning("Only supported on Raspberry Pi.")
            return

        exception_count = 0

        # Main loop to record video and take still images
        while self.continue_recording():
            try:
                # Record video for the specified number of seconds
                start_time = api.utc_now()

                # Get the filename for the video file
                filename = file_naming.get_temporary_filename(self.recording_format)

                # Replace the FILENAME placeholder in the command with the actual filename
                cmd = self.rpicam_cmd.replace("FILENAME", str(filename))

                # If the "--camera SENSOR_INDEX" string is present, replace SENSOR_INDEX with
                # the actual sensor index
                if "--camera SENSOR_INDEX" in cmd:
                    cmd = cmd.replace("SENSOR_INDEX", str(self.sensor_index))

                logger.info(f"Recording video with command: {cmd}")

                # Start the video recording process
                rc = utils.run_cmd(cmd)
                logger.info(f"Video recording completed with rc={rc}")

                # Save the video file to the datastream
                self.save_recording(RPICAM_STILL_STREAM_INDEX, 
                                    filename, 
                                    start_time=start_time, 
                                    end_time=api.utc_now())
                
                exception_count = 0  # Reset exception count on success

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in RpicamSensor: {e}", exc_info=True)
                exception_count += 1

                # On the assumption that the error is transient, we will continue to run but sleep for 60s
                self.stop_requested.wait(60)
                if exception_count > 30:
                    logger.error(f"RpicamSensor has failed {exception_count} times. Exiting.")
                    self.sensor_failed()
                    break
            finally:
                logger.debug("RpicamStillSensor loop iteration complete")
                self.stop_requested.wait(self.recording_interval_seconds)

        logger.warning("Exiting RpicamStillSensor loop")
