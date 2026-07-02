import threading
from time import sleep

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.utils import utils

logger = root_cfg.setup_logger("expidite")

# How long to wait for the orchestrator to stop before rebooting anyway. Bounded by sensor shutdown
# (max_recording_timer, default 180s) plus journal flush and the upload queue's bounded network flush
# (SPOOL_SHUTDOWN_FLUSH_SECONDS); anything unsent by then has been spilled to the disk spool.
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
    collect_diagnostics: bool = False,
) -> None:
    """Stop RpiCore gracefully (bounded), then reboot the device.

    background=True (the default) performs the wait-and-reboot on a daemon thread and returns immediately.
    This is required when called from a thread that EdgeOrchestrator.stop_all() joins (the device manager,
    sensor threads, health checks): blocking such a thread on the shutdown it just triggered would
    deadlock. Use background=False only from outside the RpiCore process (e.g. BCLI).

    delay_seconds delays the whole sequence, e.g. so an IoT Hub method response can be delivered first.

    collect_diagnostics=True gathers a diagnostics bundle before stopping. Set it for reboots that are
    recovery actions from a fault (wifi outage, memory exhaustion, stale HEART); user-requested reboots
    (BCLI, IoT Hub command) don't collect diagnostics because there is nothing wrong to diagnose.
    """
    if not root_cfg.running_on_rpi:
        logger.warning(f"Ignoring managed reboot request ({reason}); not running on a Raspberry Pi")
        return

    logger.error(f"{root_cfg.RAISE_WARN()}Rebooting device: {reason}")

    if background:
        threading.Thread(
            target=_flush_and_reboot,
            args=(reason, delay_seconds, collect_diagnostics),
            name="managed_reboot",
            daemon=True,
        ).start()
    else:
        _flush_and_reboot(reason, delay_seconds, collect_diagnostics)


def _flush_and_reboot(reason: str, delay_seconds: float, collect_diagnostics: bool) -> None:
    # Imported lazily to avoid circular imports (DiagnosticsBundle and EdgeOrchestrator both sit above
    # several modules that need to request reboots).
    from expidite_rpi.core.diagnostics_bundle import DiagnosticsBundle
    from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator

    try:
        if delay_seconds > 0:
            sleep(delay_seconds)

        if collect_diagnostics:
            # Collect diagnostics while the system is still running.
            DiagnosticsBundle.collect(reason)

        # Request a graceful stop and wait (bounded) for the orchestrator to finish flushing. The flag is
        # polled every second by the EdgeOrchestrator main loop; if RpiCore isn't running (or is in another
        # process), watchdog_file_alive() tracks it via flag files so this works cross-process too.
        root_cfg.STOP_EXPIDITE_FLAG.touch()
        waited = 0.0
        while EdgeOrchestrator.watchdog_file_alive() and waited < _REBOOT_FLUSH_TIMEOUT_SECONDS:
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
