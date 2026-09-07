##############################################################################################################
# Execute environment dependent setup
##############################################################################################################
import contextlib
import datetime as dt
import os
import re
import signal
import subprocess
from datetime import UTC
from pathlib import Path
from threading import Lock, Timer

import psutil

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

##############################################################################################################
# Functions used by sensors.
##############################################################################################################

last_space_check = dt.datetime(1970, 1, 1, tzinfo=UTC)
last_space_check_value = 0.0
last_space_check_outcome = False
last_space_check_breakdown = ""
last_temp_check = dt.datetime(1970, 1, 1, tzinfo=UTC)
last_temp_check_outcome = False
_check_lock = Lock()  # Guards the cached readings above, their outcomes and their clocks.
CRITICAL_EXPIDITE_MOUNT_THRESHOLD = 75.0
HIGH_EXPIDITE_MOUNT_THRESHOLD = 25.0
# Raspberry Pi throttles the CPU beginning at 80℃ (soft throttling) and more aggressively at 85℃ (hard
# throttling), so we reduce load before that point.
HIGH_TEMPERATURE_THRESHOLD = 75.0
# How long a reading is reused for, covering both the mount-usage and the CPU-temperature caches below.
CHECK_INTERVAL_S = 30.0


def _summarise_mount_usage() -> str:
    """Return a one-line breakdown of what is occupying ROOT_WORKING_DIR, biggest directory first.

    Each pipeline stage owns a top-level directory (processing, staging, upload, tmp, logs, and spool when
    the persistent spool has failed over), so naming them says whether the mount is backed up behind a
    DataProcessor, behind the cloud, or is just log churn.

    Best-effort: files are created and deleted under us while we walk, so anything that vanishes mid-walk
    is skipped rather than raising into the caller's warning path.
    """
    entries: list[tuple[int, int, str]] = []
    try:
        children = list(root_cfg.ROOT_WORKING_DIR.iterdir())
    except OSError as e:
        return f"breakdown unavailable ({e})"

    for child in children:
        size = 0
        count = 0
        with contextlib.suppress(OSError):
            if child.is_dir():
                for path in child.rglob("*"):
                    with contextlib.suppress(OSError):
                        if path.is_file():
                            size += path.stat().st_size
                            count += 1
            elif child.is_file():
                size = child.stat().st_size
                count = 1
        entries.append((size, count, child.name))

    if not entries:
        return "mount is empty"

    entries.sort(reverse=True)
    return ", ".join(
        f"{name} {size / 1000000:.1f}MB in {count} file{'' if count == 1 else 's'}"
        for size, count, name in entries
    )


def _refresh_space_check() -> bool:
    """Refresh the cached mount usage if it is stale, and return whether usage is critical.

    The low-disk warning belongs here, with the refresh, rather than in failing_to_keep_up(). Both public
    checks share one cached reading and one clock, so whichever of them refreshes first resets that clock
    for the other. With the warning in failing_to_keep_up() alone, a reduce_load_advised() call winning
    the window would refresh silently and leave failing_to_keep_up() stalling every sensor thread on a
    cached True that was never reported - the stall was invisible in the fleet logs.
    """
    global last_space_check, last_space_check_value, last_space_check_outcome
    global last_space_check_breakdown

    with _check_lock:
        now = api.utc_now()
        if (now - last_space_check).total_seconds() < CHECK_INTERVAL_S:
            return last_space_check_outcome

        last_space_check_value = psutil.disk_usage(str(root_cfg.ROOT_WORKING_DIR)).percent
        last_space_check_outcome = last_space_check_value > CRITICAL_EXPIDITE_MOUNT_THRESHOLD
        last_space_check = now
        usage = last_space_check_value
        is_critical = last_space_check_outcome

    breakdown = _summarise_mount_usage() if usage > HIGH_EXPIDITE_MOUNT_THRESHOLD else ""
    with _check_lock:
        last_space_check_breakdown = breakdown

    if is_critical:
        logger.warning(
            f"{root_cfg.RAISE_WARN()}Failing to keep up due to low disk space: expidite mount usage "
            f"{usage}% (threshold {CRITICAL_EXPIDITE_MOUNT_THRESHOLD}%); largest contents: {breakdown}"
        )

    return is_critical


def failing_to_keep_up() -> bool:
    """Function that allows us to back off intensive operations if we're running low on space."""
    if not root_cfg.running_on_rpi:
        return False

    return _refresh_space_check()


def reduce_load_advised() -> bool:
    """Function that allows us to back off intensive operations if we're running under high load."""
    global last_temp_check, last_temp_check_outcome

    if not root_cfg.running_on_rpi:
        return False

    _refresh_space_check()

    with _check_lock:
        now = api.utc_now()
        if (now - last_temp_check).total_seconds() < CHECK_INTERVAL_S:
            return last_temp_check_outcome

        cpu_readings = psutil.sensors_temperatures().get("cpu_thermal")  # type: ignore
        cpu_temp = cpu_readings[0].current if cpu_readings else 0

        cpu_too_hot = cpu_temp > HIGH_TEMPERATURE_THRESHOLD
        mount_too_full = last_space_check_value > HIGH_EXPIDITE_MOUNT_THRESHOLD

        if cpu_too_hot:
            logger.warning(
                f"{root_cfg.RAISE_WARN()}Advising to reduce load due to high CPU temperature "
                f"{cpu_temp}℃ (threshold {HIGH_TEMPERATURE_THRESHOLD}℃)"
            )
        if mount_too_full:
            logger.warning(
                f"{root_cfg.RAISE_WARN()}Advising to reduce load due to high expidite mount usage "
                f"{last_space_check_value}% (threshold {HIGH_EXPIDITE_MOUNT_THRESHOLD}%); "
                f"largest contents: {last_space_check_breakdown or 'not yet sampled'}"
            )

        last_temp_check_outcome = cpu_too_hot or mount_too_full
        last_temp_check = now

        return last_temp_check_outcome


##############################################################################################################
# Run a system command and return the output, or throw an exception on bad return code
##############################################################################################################
def run_cmd(
    cmd: str,
    ignore_errors: bool = False,
    grep_strs: list[str] | None = None,
    timeout: float | None = None,
) -> str:
    """Run a system command and return the output, or throw an exception on bad return code.

    Parameters:
        cmd: str
            The command to run. This should be a string that can be passed to the shell.
        ignore_errors: bool
            If True, ignore errors and return an empty string. If False, raise an exception on error.
        grep_strs: list[str]
            A list of strings to grep for in the output. If None, return the full output.
            If not None, return only the lines that contain all of the strings in the list.
        timeout: float | None
            Maximum number of seconds to wait for the command to complete. If None (the default), wait
            indefinitely. If the command does not complete within the timeout, the whole process group is
            killed (so shell-spawned children such as rpicam-vid are terminated, not orphaned) and the
            command is treated as a failure (return "" if ignore_errors, otherwise raise).

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
        from expidite_rpi.utils.rpi_emulator import RpiEmulator

        harness = RpiEmulator.get_instance()
        return harness.run_cmd_test_stub(cmd, ignore_errors, grep_strs)

    if root_cfg.running_on_windows:
        assert ignore_errors, "run_cmd is not fully supported on Windows"

    try:
        # start_new_session puts the shell and any children it spawns into a new process group, so that on
        # timeout we can kill the whole group rather than leaving children (e.g. rpicam-vid) running.
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            start_new_session=not root_cfg.running_on_windows,
        )
        try:
            out, err = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            _kill_process_group(p)
            # communicate() again to reap the killed process and drain its pipes. SIGKILL closes the
            # children's pipe ends so this normally returns at once, but bound it anyway: a process wedged
            # in the kernel (e.g. a camera driver in uninterruptible sleep, which only acts on SIGKILL once
            # the syscall returns) would otherwise keep the pipe open and re-hang us here. If the reap also
            # times out we give up on it and free this thread; the leaked child needs a restart/reboot.
            with contextlib.suppress(subprocess.TimeoutExpired):
                p.communicate(timeout=10)
            msg = f"{root_cfg.RAISE_WARN()}Command timed out after {timeout}s and was killed: {cmd}"
            if ignore_errors:
                logger.warning(msg)
                return ""
            raise RuntimeError(msg) from None

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


def _kill_process_group(p: "subprocess.Popen[bytes]") -> None:
    """Kill the process group led by p so that shell-spawned children are terminated too.

    On POSIX the process was started with start_new_session=True, giving it its own process group whose id
    equals the shell's pid; SIGKILL to that group takes down the shell and any children (e.g. rpicam-vid).
    On Windows (only reached in ignore_errors mode) we fall back to killing the process directly.
    """
    if root_cfg.running_on_windows:
        with contextlib.suppress(Exception):
            p.kill()
        return
    # os.killpg / os.getpgid / signal.SIGKILL are POSIX-only; this branch only runs on Linux.
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)  # type: ignore


# Default duration (seconds) assumed for a video/still command that does not specify a "-t" timeout.
VIDEO_CMD_DEFAULT_DURATION_S: float = 180.0
# Extra grace (seconds) added on top of the command's own duration before we treat it as hung. This is a
# safety backstop to turn an infinite hang into a normal exception, not a tuning knob, so keep it generous.
VIDEO_CMD_TIMEOUT_MARGIN_S: float = 180.0


def run_video_cmd(
    cmd: str,
    ignore_errors: bool = False,
    grep_strs: list[str] | None = None,
    default_duration_s: float = VIDEO_CMD_DEFAULT_DURATION_S,
    margin_s: float = VIDEO_CMD_TIMEOUT_MARGIN_S,
) -> str:
    """Run an rpicam-style camera command with a timeout derived from its own "-t" duration.

    rpicam-vid / rpicam-still occasionally hang at the libcamera/hardware level and ignore their own "-t"
    timer, leaving the calling sensor thread blocked forever inside run_cmd. This wrapper parses the
    command's "-t <milliseconds>" duration (falling back to default_duration_s when absent, and passes run_cmd
    a timeout of duration + margin, so a hung camera becomes a bounded, recoverable exception rather than
    permanentlt stuck.
    """
    match = re.search(r"\s-t\s+(\d+)", cmd)
    duration_s = int(match.group(1)) / 1000 if match else default_duration_s
    return run_cmd(cmd, ignore_errors=ignore_errors, grep_strs=grep_strs, timeout=duration_s + margin_s)


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
