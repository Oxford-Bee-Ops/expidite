####################################################################################################
# Sensor class that provides a direct map onto Raspberry Pi's rpicam-vid for continuous video recording.
#
# The user specifies the rpicam-vid command line, except for the file name, which is set by RpiCore.
#
####################################################################################################

from dataclasses import dataclass
from typing import cast

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

RPICAM_DATA_TYPE_ID = "RPICAM"
RPICAM_STREAM_INDEX: int = 0
RPICAM_STREAM: Stream = Stream(
            description="Basic continuous video recording.",
            type_id=RPICAM_DATA_TYPE_ID,
            index=RPICAM_STREAM_INDEX,
            format=api.FORMAT.MP4,
            cloud_container="expidite-upload",
            sample_probability="0.0",
        )
RPICAM_REVIEW_MODE_DATA_TYPE_ID = "RPICAMRM"
RPICAM_REVIEW_MODE_STREAM_INDEX: int = 1
RPICAM_REVIEW_MODE_STREAM: Stream = Stream(
            description="Review mode image stream.",
            type_id=RPICAM_REVIEW_MODE_DATA_TYPE_ID,
            index=RPICAM_REVIEW_MODE_STREAM_INDEX,
            format=api.FORMAT.JPG,
            cloud_container="expidite-review-mode",
            sample_probability="1.0",
            file_naming=api.FILE_NAMING.REVIEW_MODE,
            storage_tier=api.StorageTier.HOT,
        )

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
    review_mode_cmd: str = "rpicam-still --width 640 --height 480 -o FILENAME"

DEFAULT_RPICAM_SENSOR_CFG = RpicamSensorCfg(
    sensor_type=api.SENSOR_TYPE.CAMERA,
    sensor_index=0,
    sensor_model="PiCameraModule3",
    description="Video sensor that uses rpicam-vid",
    outputs=[RPICAM_STREAM, RPICAM_REVIEW_MODE_STREAM],
)

class RpicamSensor(Sensor):
    def __init__(self, config: RpicamSensorCfg) -> None:
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

        # Validate that the required streams exist in the configuration
        try:
            self.get_stream(RPICAM_STREAM_INDEX)  # Main video stream
        except ValueError as e:
            raise ValueError(f"RpicamSensor requires a main video stream at index "
                             f"{RPICAM_STREAM_INDEX}: {e}")

        try:
            self.get_stream(RPICAM_REVIEW_MODE_STREAM_INDEX)  # Review mode stream
        except ValueError as e:
            raise ValueError(f"RpicamSensor requires a review mode stream at index "
                             f"{RPICAM_REVIEW_MODE_STREAM_INDEX}: {e}")


    def review_mode_output(self) -> None:
        """Output an image to show the user what the camera is viewing.
        We write to the same filename every time to avoid filling up the disk and to make it
        easier for the dashboard to display the current camera view."""

        try:
            filename = file_naming.get_temporary_filename(api.FORMAT.JPG)
            config = cast(RpicamSensorCfg, self.config)

            # Run recording process
            utils.run_cmd(config.review_mode_cmd.replace("FILENAME", str(filename)))
            logger.info("Review mode image captured")
            self.save_recording(
                RPICAM_REVIEW_MODE_STREAM_INDEX,
                filename,
                start_time=api.utc_now()
            )

        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Error in RpicamSensor review_mode_output: {e}",
                         exc_info=True)


    def run(self):
        """Main loop for the RpicamSensor - runs continuously unless paused."""
        if not root_cfg.running_on_rpi and root_cfg.ST_MODE != root_cfg.SOFTWARE_TEST_MODE.TESTING:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return

        exception_count = 0

        # Main loop to record video and take still images
        while self.continue_recording():
            try:
                if self.in_review_mode():
                    self.review_mode_output()
                    self.stop_requested.wait(root_cfg.my_device.review_mode_frequency)
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

        logger.warning("Exiting RpicamSensor loop")
