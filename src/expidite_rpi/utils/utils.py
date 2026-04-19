##############################################################################################################
# Execute environment dependent setup
##############################################################################################################
import datetime as dt
import os
import subprocess
from datetime import UTC
from pathlib import Path
from threading import Timer

import psutil

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.utils.rpi_emulator import RpiEmulator

logger = root_cfg.setup_logger("expidite")

##############################################################################################################
# Functions used by sensors.
##############################################################################################################

last_space_check = dt.datetime(1970, 1, 1, tzinfo=UTC)
last_check_outcome = False
high_memory_usage_threshold = 75.0


def failing_to_keep_up() -> bool:
    """Function that allows us to back off intensive operations if we're running low on space."""
    # Cache the result for 30 seconds to avoid repeated disk checks
    global last_space_check, last_check_outcome
    now = api.utc_now()
    if (now - last_space_check).seconds < 30:
        return last_check_outcome
    last_space_check = now

    if root_cfg.running_on_rpi and (
        psutil.disk_usage(str(root_cfg.ROOT_WORKING_DIR)).percent > high_memory_usage_threshold
    ):
        logger.warning(f"{root_cfg.RAISE_WARN()} Failing to keep up due to low disk space")
        last_check_outcome = True
    else:
        last_check_outcome = False

    return last_check_outcome


##############################################################################################################
# Run a system command and return the output, or throw an exception on bad return code
##############################################################################################################
def run_cmd(cmd: str, ignore_errors: bool = False, grep_strs: list[str] | None = None) -> str:
    """Run a system command and return the output, or throw an exception on bad return code.

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

    Raises:
        Exception
            If the command fails and ignore_errors is False, raise an exception with the error message.

    """
    # In test mode, we stub out commands so that we can run more realistic test scenarios.
    if root_cfg.ST_MODE == root_cfg.SOFTWARE_TEST_MODE.TESTING:
        harness = RpiEmulator.get_instance()
        return harness.run_cmd_test_stub(cmd, ignore_errors, grep_strs)

    if root_cfg.running_on_windows:
        assert ignore_errors, "run_cmd is not fully supported on Windows"

    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = p.communicate()

        if p.returncode != 0:
            if ignore_errors:
                logger.info(f"Ignoring failure running command: {cmd} Err output: {err!s}")
                return ""
            msg = f"{root_cfg.RAISE_WARN()}Error running command: {cmd}, Error: {err!s}"
            raise RuntimeError(msg)

        # Return lines that contain all of the entries in grep_strs
        output = out.decode("utf-8").strip()
        if grep_strs is not None:
            for grep_str in grep_strs:
                output = "\n".join([x for x in output.split("\n") if grep_str in x])

        return output

    except FileNotFoundError:
        logger.exception(f"{root_cfg.RAISE_WARN()}Command not found: {cmd}")
        if ignore_errors:
            return ""
        raise


##############################################################################################################
# Timer class that repeats
##############################################################################################################
class RepeatTimer(Timer):
    def run(self) -> None:
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def get_current_user() -> str:
    """Get the current user name."""
    if root_cfg.running_on_windows:
        try:
            return os.getlogin()
        except Exception as e:
            return f"Error retrieving user: {e}"
    else:
        try:
            import pwd

            return pwd.getpwuid(os.getuid()).pw_name  # type: ignore
        except Exception as e:
            return f"Error retrieving user: {e}"


##############################################################################################################
# Utility to determine if a process is already running
#
# Looks for process_name in the list of running processes and confirms that the process ID is not the current
# process ID.
##############################################################################################################
def is_already_running(process_name: str) -> bool:
    if root_cfg.running_on_windows:
        logger.warning("is_already_running not supported on Windows")
        return False

    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if process_name in str(proc.cmdline()):
                # Check that the process ID is not our process ID
                if proc.pid != os.getpid():
                    print("Process already running:" + str(proc.cmdline()) + " PID:" + str(proc.pid))
                    return True

        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


##############################################################################################################
# Utility to check what processes are running.
#
# All the interesting ones are python ones and we can match a module string eg core.device_manager
#
# This function discards all lines and all parts of the line that don't match the module string
# It builds up a set of the module strings, discarding duplicates
##############################################################################################################
def check_running_processes(search_string: str = "core") -> set:
    if root_cfg.running_on_windows:
        logger.warning("check_running_processes not supported on Windows")
        return set()

    processes = set()
    for proc in psutil.process_iter():
        try:
            for line in proc.cmdline():
                # Parse the line into the space-separated segments
                segments = line.split(" ")
                # Find the segment that contains the search string
                for segment in segments:
                    if search_string in segment:
                        processes.add(segment)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return processes


##############################################################################################################
# Convert a file from H264 to MP4 format
##############################################################################################################
def convert_h264_to_mp4(src_file: Path, dst_file: Path) -> None:
    # Use ffmpeg to convert H264 to MP4 while maintaining image quality
    command = [
        "ffmpeg",
        "-y",  # Overwrite the output file if it exists
        "-i",
        str(src_file),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "superfast",
        "-crf",
        "18",
        str(dst_file),
    ]
    subprocess.run(command, check=True)
