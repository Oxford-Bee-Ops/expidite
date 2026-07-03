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
# All deliberate reboots (wifi recovery, memory exhaustion, stale HEART, IoT Hub command, BCLI) must go
# through request_managed_reboot so that queued sensor data is flushed before the device goes down. Data
# awaiting upload lives in RAM (the async upload queue, and /expidite which is a tmpfs on SD-card devices),
# so a bare `sudo reboot` would lose all of it. Setting STOP_EXPIDITE_FLAG makes the EdgeOrchestrator main
# loop stop cleanly: sensors stop, journals flush, and unsent uploads are spilled to the persistent disk
# spool, from which they are uploaded after the reboot.
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
