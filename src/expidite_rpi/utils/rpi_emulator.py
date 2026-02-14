##############################################################################################################
# The test harness enables thorough testing of the sensor code without RPi hardware.
# It emulates / intercepts:
# - the run_cmd function to enable injection of example recordings that then flow through the system
# - the CloudConnnector to store results locally (this is done via the LocalCloudConnector)
#
# It provides utilities to interrogate the local output and check that the expected data is present.
##############################################################################################################
import shlex
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from types import TracebackType

import cv2
import numpy as np
import pandas as pd

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import CloudConnector, LocalCloudConnector
from expidite_rpi.core.device_config_objects import DeviceCfg

logger = root_cfg.setup_logger("expidite")


@dataclass
class RpiTestRecording:
    cmd_prefix: str
    recordings: list[Path]


class RpiEmulator:
    """The test harness enables thorough testing of the sensor code without RPi hardware."""

    _instance = None
    _is_available = Event()
    ONE_OR_MORE = -1

    def __init__(self) -> None:
        self.recordings: list[RpiTestRecording] = []
        self.previous_recordings_index: int = 0
        self.recordings_saved: dict[str, int] = {}
        self.recording_cap: int = -1
        self.recording_cap_dict: dict[str, int] = {}
        self.inventory: list[DeviceCfg] = []

    @staticmethod
    def get_instance() -> "RpiEmulator":
        """Get the singleton instance of RpiEmulator."""
        if RpiEmulator._instance is None:
            RpiEmulator._instance = RpiEmulator()
            RpiEmulator._is_available.set()
        return RpiEmulator._instance

    def __enter__(self) -> "RpiEmulator":
        """Enter the context manager."""
        logger.info("Entering RpiEmulator context.")
        # We want to avoid overlapping tests so we wait until the previous test has finished
        while not RpiEmulator._is_available.is_set():
            RpiEmulator._is_available.wait()
        RpiEmulator._is_available.clear()
        self.previous_recordings_index = 0
        self.recordings_saved = {}
        self.recording_cap = -1
        self.recording_cap_dict = {}
        root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING
        root_cfg.CLOUD_TYPE = root_cfg.CloudType.LOCAL_EMULATOR
        cc = CloudConnector.get_instance(root_cfg.CLOUD_TYPE)
        assert isinstance(cc, LocalCloudConnector)
        self.cc: LocalCloudConnector = cc
        self.local_cloud = self.cc.get_local_cloud()  # Newly created local cloud

        # Mock system timers so tests run faster
        root_cfg.DP_FREQUENCY = 1
        root_cfg.JOURNAL_SYNC_FREQUENCY = 1
        root_cfg.WATCHDOG_FREQUENCY = 1

        # Ensure the processing directory is empty
        root_cfg.EDGE_PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
        for file in root_cfg.EDGE_PROCESSING_DIR.glob("*"):
            if file.is_file():
                file.unlink()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager."""
        logger.info("Exiting RpiEmulator context.")
        # self.cc.clear_local_cloud()
        RpiEmulator._is_available.set()

    def mock_timers(self, inventory: list[DeviceCfg]) -> list[DeviceCfg]:
        for device in inventory:
            # Mock the timers for each device
            device.env_sensor_frequency = 1
            device.heart_beat_frequency = 1
            device.max_recording_timer = 5
        return inventory

    ##########################################################################################################
    # Test harness functions
    ##########################################################################################################
    def set_recordings(self, recordings: list[RpiTestRecording]) -> None:
        """Set the recordings to be used for testing.

        Call this function to specify which recording should be returned in which conditions.
        """
        for recording in recordings:
            # Check the recording exists
            for rec in recording.recordings:
                if not rec.exists():
                    msg = f"Recording {rec} does not exist"
                    raise ValueError(msg)
        self.recordings = recordings

    def set_recording_cap(self, cap: int, type_id: str | None = None) -> None:
        """Set the maximum number of recordings to be saved.

        If a type_id is provided, set the cap for that type only.
        """
        if type_id is not None:
            self.recording_cap_dict[type_id] = cap
        else:
            self.recording_cap = cap

    def recordings_cap_hit(self, type_id: str) -> bool:
        """Check if the recording cap has been hit for the given type_id."""
        if type_id in self.recording_cap_dict:
            cap = self.recording_cap_dict[type_id]
        else:
            cap = self.recording_cap
        if cap == -1:
            raise ValueError("The recording cap is not set for that type_id. You'll be waiting forever!")
        return self.recordings_saved.get(type_id, 0) >= cap

    @staticmethod
    def recordings_still_to_process() -> bool:
        """Check if there are recordings remaining to process."""
        # Look for any files remaining in the processing directory
        return any(file.is_file() for file in root_cfg.EDGE_PROCESSING_DIR.glob("*"))

    @staticmethod
    def fix_recording_device_id(fname: Path) -> Path:
        """We use real recordings in system test which means they have the wrong
        device ID. We want to replace the device_id with that of this device otherwise
        expidite won't find the recordings in the EDGE_PROCESSING_DIR.
        """
        # Get the current device ID
        current_device_id = root_cfg.my_device.device_id

        # The device id is always the 3rd part of the filename
        parts = fname.name.split("_")
        if len(parts) < 3:
            msg = f"Filename {fname} does not have enough parts to contain a device ID."
            raise ValueError(msg)

        # Replace the device ID with the current device ID
        parts[2] = current_device_id
        return fname.parent / "_".join(parts)

    def assert_records(
        self, container: str, expected_files: dict[str, int], expected_rows: dict[str, int] | None = None
    ) -> None:
        """Assert that the expected number of files exist.

        Parameters:
            container: str
                The name of the cloud storage container to check.
            expected_files: dict[str, int]
                A dictionary with the expected number of files for each file name prefix.
                The keys are the prefixes of the file names.
                The values are the expected number of files.
            expected_rows: dict[str, int]
                A dictionary with the expected number of data rows in each file with the given name prefix.
                The value excludes the header row.
        """
        assert self.local_cloud is not None, (
            "Local cloud not set. Use RpiEmulator as a context manager to set it."
            "with RpiEmulator.get_instance() as scem: "
            "   ..."
        )
        for file_prefix, count in expected_files.items():
            if not file_prefix.endswith("*"):
                file_prefix = file_prefix + "*"
            files = list((self.local_cloud / container).glob(file_prefix))
            if count == self.ONE_OR_MORE:
                # Check that at least one file exists with the prefix
                assert len(files) > 0, (
                    f"Expected at least one file with prefix {file_prefix}, but found no files."
                )
            else:
                # Check that the exact number of files exists with the prefix
                # We use len(files) == count to check for exact match
                # This is because we may have multiple recordings of the same type
                assert len(files) == count, (
                    f"Expected {count} files with prefix {file_prefix}, but found {len(files)} files."
                )
        if expected_rows is not None:
            for file_prefix, count in expected_rows.items():
                files = list((self.local_cloud / container).glob(file_prefix))
                for file in files:
                    # Check the number of data rows in the file
                    with open(file) as f:
                        lines = f.readlines()
                        # Ignore any blank lines at the end of the file
                        lines = [line for line in lines if line.strip()]
                        # Assume the first line is the header
                        lines = lines[1:]
                        assert len(lines) == count, (
                            f"Expected {count} rows in file {file}, but found {len(lines)} rows."
                        )

    def get_journal_as_df(self, container: str, file_prefix: str) -> pd.DataFrame:
        """Get the journal specified by the container & file_prefix and return as a pandas DataFrame
        for further custom validation.
        """
        assert self.local_cloud is not None, (
            "Local cloud not set. Use RpiEmulator as a context manager to set it."
            "with RpiEmulator.get_instance() as scem: "
            "   ..."
        )

        if not file_prefix.endswith("*"):
            file_prefix += "*"
        files = list((self.local_cloud / container).glob(file_prefix))

        # Just return the first file found
        if len(files) == 0:
            msg = f"No files found with prefix {file_prefix}"
            raise FileNotFoundError(msg)
        if len(files) > 1:
            logger.warning(f"Multiple files found with prefix {file_prefix}. Using the first one: {files[0]}")
        file = files[0]

        # Read the file into a pandas DataFrame
        return pd.read_csv(file, skip_blank_lines=True)

    @staticmethod
    def record_system_test_run(test_name: str, test_input: dict) -> None:
        """Record the system test run to the cloud."""
        # The keys.env cloud_storage_account will be set to the system test account
        # We record this test run to an st journal in the storage account
        test_input["test_name"] = test_name
        test_input["test_time"] = api.utc_to_iso_str()
        fname = file_naming.get_system_test_filename(test_name)
        pd.DataFrame([test_input]).to_csv(fname)

        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        cc.append_to_cloud(
            dst_container=root_cfg.my_device.cc_for_system_test, src_file=fname, delete_src=True
        )

    ##########################################################################################################
    # Internal implementation functions
    ##########################################################################################################
    def _match_recording(self, cmd: str) -> list[Path] | None:
        """Check if the command matches any of the recordings.

        Parameters:
            cmd: str
                The command to run. This should be a string that can be passed to the shell.

        Returns:
            Path | None
                The path to the recording file if a match is found, None otherwise.
        """
        for recording in self.recordings:
            if cmd.startswith(recording.cmd_prefix):
                return recording.recordings
        return None

    def ok_to_save_recording(self, type_id: str) -> bool:
        """Check if we are allowed to save a recording.

        We have to check both the global recording cap and the per-type recording cap.
        """
        previous_recordings = self.recordings_saved.get(type_id, 0)
        type_cap = self.recording_cap_dict.get(type_id, self.recording_cap)

        if (type_cap == -1) or (previous_recordings < type_cap):
            pass_check = True
        else:
            pass_check = False

        if pass_check:
            self.recordings_saved[type_id] = previous_recordings + 1
            return True
        logger.debug(
            f"Recording cap exceeded for {type_id}. "
            f"Global cap: {self.recording_cap}, "
            f"Type cap: {self.recording_cap_dict.get(type_id, -1)}, "
            f"Previous recordings: {previous_recordings}"
        )
        return False

    ##########################################################################################################
    # Sensor command emulation
    ##########################################################################################################
    def run_cmd_test_stub(
        self, cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None
    ) -> str:
        """For testing purposes, we emulate certain basic Linux sensor commands so that we can run more
        realistic test scenarios on Windows.

        We currently emulate:
        - rpicam-vid

        Parameters:
            cmd: str
                The command to run. This should be a string that can be passed to the shell.
            ignore_errors: bool
                If True, ignore errors and return an empty string. If False, raise an exception on error.
            grep_strs: list[str]
                A list of strings to grep for in the output. If None, return the full output.
                If not None, return only the lines that contain all of the strings in the list.

        Returns:
            str
                The output of the command. If ignore_errors is True, return an empty string on error.
                If grep_strs is not None, return only the lines that contain all of the strings in the list.
        """
        if cmd.startswith("rpicam-vid"):
            return self.emulate_rpicam_vid(cmd, ignore_errors, grep_strs)

        if cmd.startswith("arecord"):
            # Emulate the arecord command
            # This is a simple emulation that just returns a success code and a message.
            # In a real scenario, we would run the command and return the output.
            return self.emulate_arecord(cmd, ignore_errors, grep_strs)

        return "Command not run on windows: " + cmd

    def emulate_rpicam_vid(
        self, cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None
    ) -> str:
        # Emulate the rpicam-vid command
        # We expect commands like:
        #  "rpicam-vid --framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0"
        #
        # We try to find a matching recording to provide.
        #
        # If we fail, we create a video file with:
        # - filename taken from the -o parameter
        # - duration taken from the -t parameter (in milliseconds)
        # - framerate taken from the --framerate parameter
        # - width taken from the --width parameter
        # - height taken from the --height parameter
        args = shlex.split(cmd, posix=False)
        if args.index("-o") == -1 or args.index("-t") == -1:
            raise ValueError("Missing required arguments in command: " + cmd)

        filename = args[args.index("-o") + 1]
        suffix = filename.split(".")[-1]
        duration = int(args[args.index("-t") + 1]) / 1000  # Convert to seconds

        # We divide duration to get a 25x speedup for testing purposes
        duration = int(duration / 25)

        if "--framerate" not in args:
            framerate = 30  # Default framerate
        else:
            framerate = int(args[args.index("--framerate") + 1])
        if "--width" not in args:
            width = 640
        else:
            width = int(args[args.index("--width") + 1])
        if "--height" not in args:
            height = 480
        else:
            height = int(args[args.index("--height") + 1])

        # See if we have a matching cmd in the recordings list
        # We need to replace the filename with FILENAME
        match_cmd = cmd.replace(filename, "FILENAME")
        if "--camera" in match_cmd:
            parts = match_cmd.split("--camera")
            match_cmd = parts[0] + " SENSOR_INDEX" + parts[1][2:]
        recordings = self._match_recording(cmd)
        logger.debug(f"Found match command {recordings is not None} for match command: {match_cmd}")

        if recordings:
            # We have a recording so save that with the appropriate filename
            recording = recordings[self.previous_recordings_index]
            self.previous_recordings_index += 1
            self.previous_recordings_index %= len(recordings)
            shutil.copy(recording, filename)
            logger.info(f"Recording {recording} saved to DS")
        else:
            # No recording. Create a dummy video file.
            # Use OpenCV to create a dummy video file
            if suffix == "h264":
                fourcc = cv2.VideoWriter.fourcc(*"h264")
            elif suffix == "mp4":
                fourcc = cv2.VideoWriter.fourcc(*"mp4v")
            else:
                raise ValueError("Unsupported video format: " + suffix)

            out = cv2.VideoWriter(filename, fourcc, framerate, (width, height))
            num_frames = int(framerate * duration)
            for i in range(num_frames):
                # Create a dummy frame (e.g., a solid color or gradient)
                frame = np.zeros((height, width, 3), dtype=np.uint8)
                frame[:] = (i % 256, (i * 2) % 256, (i * 3) % 256)  # Example gradient
                out.write(frame)

            # Release the VideoWriter
            out.release()
            logger.info(f"Recording generated: {filename}")

        # Sleep for the duration of the video to simulate recording time.
        time.sleep(duration)
        return f"rpicam-vid command emulated successfully, created {filename}"

    def emulate_arecord(
        self, cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None
    ) -> str:
        # Emulate the arecord command
        # We expect commands like:
        #   f"arecord -D hw:{dev_index!s} -r {samp_rate!s} -c {chans!s}"
        #   f" -f S16_LE -t wav -d {length_to_record!s} {wav_output_filename!s}"
        #
        # We try to find a matching recording to provide.
        args = shlex.split(cmd, posix=False)
        filename = args[-1]
        duration = int(args[args.index("-d") + 1])

        # We divide duration to get a 25x speedup for testing purposes
        duration = int(duration / 25)

        # See if we have a matching cmd in the recordings list
        # We need to replace the filename with FILENAME
        recordings = self._match_recording(cmd)
        logger.debug(f"Found match command {recordings is not None} for match command: {cmd}")

        if recordings:
            # We have a recording so save that with the appropriate filename
            recording = recordings[self.previous_recordings_index]
            self.previous_recordings_index += 1
            self.previous_recordings_index %= len(recordings)
            shutil.copy(recording, filename)
            logger.info(f"Recording {recording} saved to DS")
        else:
            logger.error(f"Recording not found for command: {cmd}")
            if not ignore_errors:
                msg = f"Recording not found for command: {cmd}"
                raise FileNotFoundError(msg)

        # Sleep for the duration of the video to simulate recording time.
        time.sleep(duration)
        return f"arecord command emulated successfully, created {filename}"
