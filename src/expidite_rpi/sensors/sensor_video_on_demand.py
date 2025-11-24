####################################################################################################
# Sensor class that provides on-demand video recording.
# This is a direct map onto Raspberry Pi's rpicam-vid.
#
# The user specifies the rpicam-vid command line, except for the file name, which is set by RpiCore.
#
####################################################################################################
from dataclasses import dataclass

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

VIDEO_OD_DATA_TYPE_ID = "VIDEOOD"
VIDEO_OD_STREAM_INDEX: int = 0
VIDEO_OD_STREAM: Stream = Stream(
            description="Video recording on demand, triggered via the BCLI.",
            type_id=VIDEO_OD_DATA_TYPE_ID,
            index=VIDEO_OD_STREAM_INDEX,
            format=api.FORMAT.MP4,
            cloud_container="expidite-od",
            sample_probability="1.0",
        )

@dataclass
class VideoOnDemandSensorCfg(SensorCfg):
    ############################################################
    # Add custom fields
    ############################################################
    # Defines the rpicam-vid command to use to record video.
    # This should be as specified in the rpicam-vid documentation.
    # The filename should be substituted with FILENAME.
    # The recording duration after -t should be substituted with DURATION in seconds.
    # Example: "rpicam-vid --framerate 5 --width 640 --height 640 -o FILENAME -t DURATION"
    # Lens position set for focusing at 30cm (where it's best to focus slightly closer at 0.25m)
    video_od_cmd: str = (f"rpicam-vid --lens-position 4 --framerate 10 "
                         f"--width 1080 --height 1080 -o FILENAME -t DURATION")

DEFAULT_VIDEO_OD_SENSOR_CFG = VideoOnDemandSensorCfg(
    sensor_type=api.SENSOR_TYPE.CAMERA,
    sensor_index=0,
    sensor_model="PiCameraModule3",
    description="Video sensor that uses rpicam-vid for on-demand recording.",
    outputs=[VIDEO_OD_STREAM],
)

class VideoOnDemandSensor(Sensor):
    def __init__(self, config: VideoOnDemandSensorCfg):
        """Constructor for the VideoOnDemandSensor class"""
        super().__init__(config)
        self.config = config
        self.recording_format = self.get_stream(VIDEO_OD_STREAM_INDEX).format
        self.video_od_cmd = self.config.video_od_cmd

        assert self.video_od_cmd, (
            f"video_od_cmd must be set in the sensor configuration: {self.video_od_cmd}"
        )
        assert self.video_od_cmd.startswith("rpicam-vid "), (
            f"video_od_cmd must start with 'rpicam-vid ': {self.video_od_cmd}"
        )
        assert "FILENAME" in self.video_od_cmd, (
            f"FILENAME placeholder missing in video_od_cmd: {self.video_od_cmd}"
        )
        assert "DURATION" in self.video_od_cmd, (
            f"DURATION placeholder missing in video_od_cmd: {self.video_od_cmd}"
        )

        # Validate that the required streams exist in the configuration
        try:
            self.get_stream(VIDEO_OD_STREAM_INDEX)  # Main video stream
        except ValueError as e:
            raise ValueError(f"VideoOnDemandSensor requires a main video stream at index "
                             f"{VIDEO_OD_STREAM_INDEX}: {e}")

    def sensing_triggered(self, duration: int) -> None:
        """Invoked by Sensor super-class when sensing is triggered."""
        if not root_cfg.running_on_rpi and root_cfg.ST_MODE != root_cfg.SOFTWARE_TEST_MODE.TESTING:
            logger.warning("Video configuration is only supported on Raspberry Pi.")
            return

        try:
            # Record video for the specified number of seconds
            start_time = api.utc_now()

            # Get the filename for the video file
            filename = file_naming.get_temporary_filename(self.recording_format)

            # Replace the FILENAME placeholder in the command with the actual filename
            cmd = self.video_od_cmd.replace("FILENAME", str(filename))

            # Replace the DURATION placeholder in the command with the actual duration in ms
            duration_ms = duration * 1000
            cmd = cmd.replace("DURATION", str(duration_ms))

            logger.info(f"Recording video with command: {cmd}")

            # Start the video recording process
            rc = utils.run_cmd(cmd)
            logger.info(f"Video recording completed with rc={rc}")

            # Save the video file to the datastream
            self.save_recording(VIDEO_OD_STREAM_INDEX,
                                filename,
                                start_time=start_time,
                                end_time=api.utc_now())

        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Error in VideoOnDemandSensor: {e}", exc_info=True)
