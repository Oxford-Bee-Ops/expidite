##############################################################################################################
# Sensor class that provides a direct map onto Raspberry Pi's rpicam-vid for continuous video recording.
#
# The user specifies the rpicam-vid command line, except for the file name, which is set by RpiCore.
#
##############################################################################################################

import json
import shlex
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
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
RPICAM_METADATA_DATA_TYPE_ID = "RPICAMMETA"
RPICAM_METADATA_STREAM_INDEX: int = 2
RPICAM_METADATA_FIELDS = ["exposure_time", "analogue_gain", "lens_position", "lux"]
RPICAM_METADATA_STREAM: Stream = Stream(
    description="First frame metadata emitted by rpicam-vid.",
    type_id=RPICAM_METADATA_DATA_TYPE_ID,
    index=RPICAM_METADATA_STREAM_INDEX,
    format=api.FORMAT.LOG,
    fields=RPICAM_METADATA_FIELDS,
    cloud_container="expidite-journals",
)


@dataclass
class RpicamSensorCfg(SensorCfg):
    ##########################################################################################################
    # Add custom fields
    ##########################################################################################################
    # Defines the rpicam-vid command to use to record video.
    # This should be as specified in the rpicam-vid documentation.
    # The filename should be substituted with FILENAME.
    # Example: "rpicam-vid --framerate 15 --width 640 --height 640 -o FILENAME -t 5000"
    # The FILENAME suffix should match the datastream input_format.
    rpicam_cmd: str = "rpicam-vid --framerate 15 --width 640 --height 480 -o FILENAME -t 5000"
    review_mode_cmd: str = "rpicam-still --width 640 --height 480 -o FILENAME"
    metadata_enabled: bool = False


DEFAULT_RPICAM_SENSOR_CFG = RpicamSensorCfg(
    sensor_type=api.SENSOR_TYPE.CAMERA,
    sensor_index=0,
    sensor_model="PiCameraModule3",
    description="Video sensor that uses rpicam-vid",
    outputs=[RPICAM_STREAM, RPICAM_REVIEW_MODE_STREAM, RPICAM_METADATA_STREAM],
)


class RpicamSensor(Sensor):
    def __init__(self, config: RpicamSensorCfg) -> None:
        """Constructor for the RpicamSensor class."""
        super().__init__(config)
        self.config = config
        self.recording_format = self.get_stream(RPICAM_STREAM_INDEX).format
        self.rpicam_cmd = self.config.rpicam_cmd
        self.metadata_enabled = self.config.metadata_enabled

        assert self.rpicam_cmd, f"rpicam_cmd must be set in the sensor configuration: {self.rpicam_cmd}"
        assert self.rpicam_cmd.startswith("rpicam-vid "), (
            f"rpicam_cmd must start with 'rpicam-vid ': {self.rpicam_cmd}"
        )
        assert "FILENAME" in self.rpicam_cmd, f"FILENAME placeholder missing in rpicam_cmd: {self.rpicam_cmd}"
        assert "FILENAME " in self.rpicam_cmd, (
            f"FILENAME placeholder should be specified without any suffix rpicam_cmd: {self.rpicam_cmd}"
        )

        # Validate that the required streams exist in the configuration
        try:
            self.get_stream(RPICAM_STREAM_INDEX)  # Main video stream
        except ValueError as e:
            msg = f"RpicamSensor requires a main video stream at index {RPICAM_STREAM_INDEX}: {e}"
            raise ValueError(msg) from e

        try:
            self.get_stream(RPICAM_REVIEW_MODE_STREAM_INDEX)  # Review mode stream
        except ValueError as e:
            msg = (
                f"RpicamSensor requires a review mode stream at index {RPICAM_REVIEW_MODE_STREAM_INDEX}: {e}"
            )
            raise ValueError(msg) from e

        try:
            self.get_stream(RPICAM_METADATA_STREAM_INDEX)  # Metadata stream
        except ValueError as e:
            msg = f"RpicamSensor requires a metadata stream at index {RPICAM_METADATA_STREAM_INDEX}: {e}"
            raise ValueError(msg) from e

    @staticmethod
    def get_metadata_filename(recording_filename: Path) -> Path:
        """Generate the temporary metadata sidecar path for a recording."""
        return recording_filename.with_suffix(".json")

    @staticmethod
    def get_metadata_output_path(cmd: str, default_path: Path) -> Path:
        """Extract the metadata output path from a concrete rpicam command string."""
        args = shlex.split(cmd, posix=False)
        for i, arg in enumerate(args):
            if arg == "--metadata" and i + 1 < len(args):
                return Path(args[i + 1])
            if arg.startswith("--metadata="):
                return Path(arg.split("=", maxsplit=1)[1])
        return default_path

    def build_recording_command(
        self, recording_filename: Path, metadata_filename: Path
    ) -> tuple[str, Path | None]:
        """Build the rpicam command and optionally emit JSON metadata to a known file."""
        cmd = self.rpicam_cmd.replace("FILENAME", str(recording_filename))
        metadata_output_path = None

        if self.metadata_enabled:
            if "--metadata" not in cmd:
                cmd = f"{cmd} --metadata {metadata_filename}"
            if "--metadata-format" not in cmd:
                cmd = f"{cmd} --metadata-format json"

            metadata_output_path = self.get_metadata_output_path(cmd, metadata_filename)

        if "--camera SENSOR_INDEX" in cmd:
            cmd = cmd.replace("SENSOR_INDEX", str(self.sensor_index))

        return cmd, metadata_output_path

    @staticmethod
    def _extract_first_frame_metadata(metadata_json: object) -> dict[str, object]:
        if isinstance(metadata_json, list):
            if len(metadata_json) == 0:
                raise ValueError("Metadata JSON list is empty")
            first_frame = metadata_json[0]
        elif isinstance(metadata_json, dict):
            metadata_dict = cast(dict[str, object], metadata_json)
            frames = metadata_dict.get("frames")
            if isinstance(frames, list):
                if len(frames) == 0:
                    raise ValueError("Metadata JSON frames list is empty")
                first_frame = frames[0]
            else:
                first_frame = metadata_dict
        else:
            msg = f"Unsupported metadata JSON type: {type(metadata_json)!r}"
            raise TypeError(msg)

        if not isinstance(first_frame, dict):
            msg = f"Expected metadata entry to be a dict, got {type(first_frame)!r}"
            raise TypeError(msg)

        return cast(dict[str, object], first_frame)

    @classmethod
    def load_first_frame_metadata(cls, metadata_file: Path) -> dict[str, object]:
        """Load the first metadata frame from a JSON sidecar file."""
        raw_metadata = metadata_file.read_text(encoding="utf-8").strip()
        if not raw_metadata:
            msg = f"Metadata file is empty: {metadata_file}"
            raise ValueError(msg)

        try:
            return cls._extract_first_frame_metadata(json.loads(raw_metadata))
        except JSONDecodeError:
            for line in raw_metadata.splitlines():
                cleaned_line = line.strip().rstrip(",")
                if cleaned_line in {"", "[", "]"}:
                    continue
                try:
                    return cls._extract_first_frame_metadata(json.loads(cleaned_line))
                except JSONDecodeError:
                    continue

        msg = f"Could not parse metadata JSON from {metadata_file}"
        raise ValueError(msg)

    def process_metadata_json_file(self, metadata_file: Path) -> None:
        """Save the first frame metadata from a rpicam JSON sidecar to the metadata stream."""
        if not metadata_file.exists():
            logger.warning(f"Metadata sidecar not found for recording: {metadata_file}")
            return

        try:
            first_frame_metadata = self.load_first_frame_metadata(metadata_file)
            sensor_data = {
                "exposure_time": first_frame_metadata.get("ExposureTime"),
                "analogue_gain": first_frame_metadata.get("AnalogueGain"),
                "lens_position": first_frame_metadata.get("LensPosition"),
                "lux": first_frame_metadata.get("Lux"),
            }
            self.log(
                stream_index=RPICAM_METADATA_STREAM_INDEX,
                sensor_data=sensor_data,
            )
        finally:
            metadata_file.unlink(missing_ok=True)

    def review_mode_output(self) -> None:
        """Output an image to show the user what the camera is viewing.
        We write to the same filename every time to avoid filling up the disk and to make it
        easier for the dashboard to display the current camera view.
        """
        try:
            filename = file_naming.get_temporary_filename(api.FORMAT.JPG)
            config = cast(RpicamSensorCfg, self.config)

            # Run recording process
            utils.run_cmd(config.review_mode_cmd.replace("FILENAME", str(filename)))
            logger.info("Review mode image captured")
            self.save_recording(RPICAM_REVIEW_MODE_STREAM_INDEX, filename, start_time=api.utc_now())

        except Exception:
            logger.exception(f"{root_cfg.RAISE_WARN()}Error in RpicamSensor review_mode_output")

    def run(self) -> None:
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
                metadata_filename = self.get_metadata_filename(filename)

                # Replace the FILENAME placeholder in the command with the actual filename and ensure
                # metadata is saved as JSON for the first-frame metadata stream.
                cmd, metadata_output_path = self.build_recording_command(filename, metadata_filename)

                logger.info(f"Recording video with command: {cmd}")

                # Start the video recording process
                rc = utils.run_cmd(cmd)
                logger.info(f"Video recording completed with rc={rc}")

                # Save the video file to the datastream
                self.save_recording(
                    RPICAM_STREAM_INDEX, filename, start_time=start_time, end_time=api.utc_now()
                )
                if metadata_output_path is not None:
                    self.process_metadata_json_file(metadata_output_path)

                exception_count = 0  # Reset exception count on success

            except Exception:
                logger.exception(f"{root_cfg.RAISE_WARN()}Error in RpicamSensor")
                exception_count += 1

                # On the assumption that the error is transient, we will continue to run but sleep for 60s
                self.stop_requested.wait(60)
                if exception_count > 30:
                    logger.exception(f"RpicamSensor has failed {exception_count} times. Exiting.")
                    self.sensor_failed()
                    break

        logger.warning("Exiting RpicamSensor loop")
