####################################################################################################
# Sensor class that provides a direct map onto Raspberry Pi's rpicam-vid for continuous video recording.
#
# The user specifies the rpicam-vid command line, except for the file name, which is set by SensorCore.
#
####################################################################################################

from dataclasses import dataclass

from sensor_core import Sensor, SensorCfg, api, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.dp_config_objects import Stream
from sensor_core.utils import utils

logger = root_cfg.setup_logger("sensor_core")

RPICAM_DATA_TYPE_ID = "RPICAM"
RPICAM_STREAM_INDEX: int = 0

@dataclass
class RpicamSensorCfg(SensorCfg):
    ############################################################
    # Add custom fields
    ############################################################
    # Defines the rpicam-vid command to use to record video.
    # This should be as specified in the rpicam-vid documentation.
    # The filename should be substituted with FILENAME. 
    # Example: "rpicam-vid --framerate 15 --width 640 --height 640 -o FILENAME -t 5000"
    # The FILENAME suffix should match the datastream input_format.
    rpicam_cmd: str = "rpicam-vid --framerate 15 --width 640 --height 480 -o FILENAME -t 5000"

DEFAULT_RPICAM_SENSOR_CFG = RpicamSensorCfg(
    sensor_type=api.SENSOR_TYPE.CAMERA,
    sensor_index=0,
    sensor_model="PiCameraModule3",
    description="Video sensor that uses rpicam-vid",
    outputs=[
        Stream(
            description="Basic continuous video recording.",
            type_id=RPICAM_DATA_TYPE_ID,
            index=RPICAM_STREAM_INDEX,
            format=api.FORMAT.MP4,
            cloud_container="sensor-core-upload",
        )
    ],
    rpicam_cmd = "rpicam-vid --framerate 15 --width 640 --height 480 -o FILENAME -t 5000",
)

class RpicamSensor(Sensor):
    def __init__(self, config: RpicamSensorCfg):
        """Constructor for the RpicamSensor class"""
        super().__init__(config)
        self.config = config
        self.recording_format = self.get_stream(RPICAM_STREAM_INDEX).format
        self.rpicam_cmd = self.config.rpicam_cmd

        assert self.rpicam_cmd, (
            f"rpicam_cmd must be set in the sensor configuration: {self.rpicam_cmd}"
        )
        assert self.rpicam_cmd.startswith("rpicam-vid "), (
            f"rpicam_cmd must start with 'rpicam-vid ': {self.rpicam_cmd}"
        )
        assert "FILENAME" in self.rpicam_cmd, (
            f"FILENAME placeholder missing in rpicam_cmd: {self.rpicam_cmd}"
        )
        assert "FILENAME " in self.rpicam_cmd, (
            f"FILENAME placeholder should be specified without any suffix rpicam_cmd: {self.rpicam_cmd}"
        )


    def run(self):
        """Main loop for the RpicamSensor - runs continuously unless paused."""
        if not root_cfg.running_on_rpi and root_cfg.TEST_MODE != root_cfg.MODE.TEST:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return

        exception_count = 0

        # Main loop to record video and take still images
        while not self.stop_requested.is_set():
            try:
                # If memory is running low, we pause recording until the downstream processing
                # catches up.
                if utils.pause_recording():
                    self.stop_requested.wait(180)
                    continue

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
                self.save_recording(RPICAM_STREAM_INDEX, 
                                    filename, 
                                    start_time=start_time, 
                                    end_time=api.utc_now())

            except FileNotFoundError as e:
                logger.error(f"{root_cfg.RAISE_WARN()}FileNotFoundError in RpicamSensor: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in RpicamSensor: {e}", exc_info=True)
                exception_count += 1
            finally:
                # On the assumption that the error is transient, we will continue to run but sleep for 60s
                self.stop_requested.wait(60)
                if exception_count > 30:
                    logger.error(f"RpicamSensor has failed {exception_count} times. Exiting.")
                    self.sensor_failed()
                    break

        logger.warning("Exiting RpicamSensor loop")
