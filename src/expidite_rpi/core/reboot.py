import threading
from time import sleep

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

# How long to wait for the orchestrator to stop before rebooting anyway. Bounded by sensor shutdown
# (max_recording_timer, default 180s) plus the journal flush; the upload queue itself is spilled to the
# disk spool at connector shutdown with no network I/O, so it adds almost nothing.
_REBOOT_FLUSH_TIMEOUT_SECONDS = 240.0


##############################################################################################################
# Managed reboot
#
# All deliberate reboots must flush queued sensor data before the device goes down: data awaiting upload
# lives in RAM (the async upload queue, and /expidite which is a tmpfs on SD-card devices), so a bare
# `sudo reboot` would lose all of it. A clean shutdown stops sensors, flushes journals, and spills unsent
# uploads to the persistent disk spool, from which they are uploaded after the reboot.
#
# There are two entry points depending on where the caller runs:
#
# - stop_service_and_reboot(): for callers OUTSIDE the RpiCore process (BCLI, the management/IoT Hub
#   service). It runs `sudo systemctl stop expidite.service` - the same graceful stop the installer uses -
#   which lets systemd SIGTERM the whole service cgroup (tearing down in-flight work such as an rpicam-vid
#   recording promptly) and blocks until the service has fully exited. This is fast and is the preferred
#   path.
#
# - request_managed_reboot(): for callers INSIDE the RpiCore process (device health / device manager
#   fault recovery). Such a caller cannot `systemctl stop` its own service without deadlocking (the stop
#   would wait for the very process that is running the stop command to exit), so instead it sets
#   STOP_EXPIDITE_FLAG - which the EdgeOrchestrator main loop polls - and waits (bounded) for the run to
#   finish before rebooting.
##############################################################################################################
def request_managed_reboot(
    reason: str,
    delay_seconds: float = 0.0,
    background: bool = True,
    is_error: bool = False,
) -> None:
    """Stop RpiCore gracefully (bounded), then reboot the device.

    delay_seconds delays the whole sequence, e.g. so an IoT Hub method response can be delivered first.

    background=True (the default) performs the wait-and-reboot on a daemon thread and returns immediately.
    This is required when called from a thread that EdgeOrchestrator.stop_all() joins (the device manager,
    sensor threads, health checks): blocking such a thread on the shutdown it just triggered would
    deadlock. Use background=False only from outside the RpiCore process (e.g. BCLI).

    is_error=True gathers a diagnostics bundle before stopping. Set it for reboots that are recovery actions
    from a fault (wifi outage, memory exhaustion, stale HEART); user-requested reboots (BCLI, IoT Hub command)
    don't collect diagnostics because there is nothing wrong to diagnose.
    """
    if not root_cfg.running_on_rpi:
        logger.warning(f"Ignoring managed reboot request ({reason}); not running on a Raspberry Pi")
        return

    if is_error:
        logger.error(f"{root_cfg.RAISE_WARN()}Rebooting device: {reason}")
    else:
        logger.warning(f"Rebooting device: {reason}")

    if background:
        threading.Thread(
            target=_flush_and_reboot,
            args=(reason, delay_seconds, is_error),
            name="managed_reboot",
            daemon=True,
        ).start()
    else:
        _flush_and_reboot(reason, delay_seconds, is_error)


def _flush_and_reboot(reason: str, delay_seconds: float, is_error: bool) -> None:
    # Imported lazily to avoid circular imports (DiagnosticsBundle sits above several modules that need to
    # request reboots).
    from expidite_rpi.core.diagnostics_bundle import DiagnosticsBundle

    try:
        if delay_seconds > 0:
            sleep(delay_seconds)

        if is_error:
            # Collect diagnostics while the system is still running.
            DiagnosticsBundle.collect(reason)

        # Request a graceful stop and wait (bounded) for the orchestrator to finish flushing.
        # We wait on the EXPIDITE_IS_RUNNING_FLAG file being *removed*: the EdgeOrchestrator main loop
        # touches it every second while running and unlinks it only after stop_all() has completed its
        # flush, so removal is a positive "fully stopped" signal that works cross-process (BCLI) too.
        # We must NOT use watchdog_file_alive() here: it reports False as soon as the STOP flag is newer
        # than the running flag - i.e. immediately after the touch below, minutes before the flush is done.
        # If the flag doesn't exist at all, RpiCore isn't running (or never started) and we reboot straight
        # away; a stale flag left by a hard-killed process costs at most the bounded timeout.
        root_cfg.STOP_EXPIDITE_FLAG.touch()
        waited = 0.0
        while root_cfg.EXPIDITE_IS_RUNNING_FLAG.exists() and waited < _REBOOT_FLUSH_TIMEOUT_SECONDS:
            sleep(1)
            waited += 1
        if waited >= _REBOOT_FLUSH_TIMEOUT_SECONDS:
            logger.error(
                f"{root_cfg.RAISE_WARN()}RpiCore did not stop within {waited:.0f}s; rebooting anyway"
            )
        else:
            logger.info(f"RpiCore stopped after {waited:.0f}s; rebooting now")
    except Exception:
        # Never let a flush failure prevent the reboot itself - the reboot is the recovery action.
        logger.exception(f"{root_cfg.RAISE_WARN()}Error flushing before reboot; rebooting anyway")

    utils.run_cmd("sudo reboot", ignore_errors=True)


# The systemd unit whose graceful stop flushes RpiCore's data. `systemctl stop` blocks until the cgroup has
# exited, escalating to SIGKILL only after the unit's TimeoutStopSec (240s); the safety timeout below sits
# above that so run_cmd never kills systemctl before systemd has finished its own bounded stop.
_EXPIDITE_SERVICE = "expidite.service"
_SERVICE_STOP_TIMEOUT_SECONDS = 300.0


def stop_service_and_reboot(reason: str, delay_seconds: float = 0.0) -> None:
    """Gracefully stop the RpiCore service via systemd, then reboot. For callers OUTSIDE the RpiCore process.

    `sudo systemctl stop expidite.service` is the same graceful stop the installer performs: systemd SIGTERMs
    the whole service cgroup - so in-flight sensor work (e.g. an rpicam-vid recording) is torn down promptly
    rather than running to completion - and blocks until the service has fully exited. RpiCore's SIGTERM
    handler turns that into a clean shutdown (sensors stop, journals flush, unsent uploads spill to the disk
    spool). The unit is left enabled, so systemd (and the @reboot crontab) auto-start RpiCore after the boot.

    Do NOT call this from within the RpiCore process: `systemctl stop` would block waiting for that very
    process to exit, so the reboot would never run. In-process reboots use request_managed_reboot instead.

    delay_seconds delays the sequence (e.g. so an IoT Hub method response can be delivered first).
    """
    if not root_cfg.running_on_rpi:
        logger.warning(f"Ignoring reboot request ({reason}); not running on a Raspberry Pi")
        return

    logger.warning(f"Rebooting device: {reason}")

    if delay_seconds != 0:
        threading.Thread(
            target=_stop_service_and_reboot,
            args=(delay_seconds,),
            name="managed_reboot",
            daemon=True,
        ).start()
    else:
        _stop_service_and_reboot(0)


def _stop_service_and_reboot(delay_seconds: float) -> None:
    try:
        if delay_seconds > 0:
            sleep(delay_seconds)
        # Blocks until the service cgroup has fully exited. ignore_errors: an already-stopped unit still
        # returns cleanly, and either way we must proceed to the reboot.
        utils.run_cmd(
            f"sudo systemctl stop {_EXPIDITE_SERVICE}",
            ignore_errors=True,
            timeout=_SERVICE_STOP_TIMEOUT_SECONDS,
        )
    except Exception:
        # Never let a stop failure prevent the reboot itself.
        logger.exception(f"{root_cfg.RAISE_WARN()}Error stopping service before reboot; rebooting anyway")

    utils.run_cmd("sudo reboot", ignore_errors=True)
