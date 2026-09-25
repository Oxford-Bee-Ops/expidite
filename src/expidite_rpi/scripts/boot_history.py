"""A deliberately tiny, standalone, best-effort boot history.

It records every boot, every requested reboot (with its reason), every orderly shutdown and every start of
RpiCore by the configured start script.

Systemd executes this file directly from the virtualenv's ``scripts`` directory. Do not change that to
``python -m expidite_rpi...`` and do not import anything from ``expidite_rpi`` here: importing a package
submodule first executes ``expidite_rpi/__init__.py``, which loads configuration, Azure and much of the
application. That is unnecessary work in the boot and shutdown paths and creates extra ways for this
diagnostic helper to fail.

For the same reason, DIAGS_DIR and its filenames are intentionally duplicated here rather than imported
from configuration, and this module uses only the Python standard library. It also deliberately has no
logging: the useful evidence is the persistent history itself, while ordinary logs do not survive the reboot.
Every public operation is best-effort so a diagnostic failure can never prevent startup, shutdown or reboot.
"""

import contextlib
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

BOOT_EVENT = "boot"
SHUTDOWN_EVENT = "shutdown"
REBOOT_REQUESTED_EVENT = "reboot_requested"
EXPIDITE_STARTED_EVENT = "expidite_started"
MAX_REASON_CHARS = 500
MAX_BYTES_PER_FILE = 100_000

DIAGS_DIR = Path("/expidite-diags")
BOOT_HISTORY_FILE = DIAGS_DIR / "boot_history.jsonl"
BOOT_HISTORY_BACKUP_FILE = DIAGS_DIR / "boot_history.1.jsonl"
_BOOT_ID_FILE = Path("/proc/sys/kernel/random/boot_id")
# Seconds since the kernel started. Unlike the wall clock, it is never stepped by NTP.
_UPTIME_FILE = Path("/proc/uptime")
_POWER_RESET_FILE = Path("/proc/device-tree/chosen/power/power_reset")
_POWER_RESET_FLAGS = {
    0: "over_voltage",
    1: "under_voltage",
    2: "over_temperature",
    3: "enable_signal",
    4: "watchdog",
}


def record_boot() -> dict[str, Any] | None:
    """Record this hardware boot.

    How the previous boot ended is deliberately not recorded here; readers derive it from the sequence. If
    a boot has a shutdown event with its boot_id, it ended in an orderly way; otherwise its end was not
    recorded.
    """
    try:
        event = _new_event(BOOT_EVENT)
        power_reset = _read_power_reset()
        if power_reset is not None:
            event["power_reset"] = power_reset
        # Rotate only at a boot boundary, so all of this boot's events share the new active file.
        _rotate_if_full()
        _append_event(event)

    except Exception:
        # Diagnostics must never prevent RpiCore from starting.
        return None
    else:
        return event


def record_shutdown() -> dict[str, Any] | None:
    """Record an orderly shutdown. Why it happened is in any reboot_requested event with the same boot_id."""
    try:
        event = _new_event(SHUTDOWN_EVENT)
        _append_event(event)
    except Exception:
        return None
    else:
        return event


def record_reboot_request(reason: str) -> dict[str, Any] | None:
    """Record that a reboot was requested, and why, just before the caller issues it.

    The event is true whether or not the reboot then succeeds, so callers never need to undo it.
    """
    try:
        event = _new_event(REBOOT_REQUESTED_EVENT)
        event["reason"] = reason[:MAX_REASON_CHARS]
        _append_event(event)
    except Exception:
        return None
    else:
        return event


def record_expidite_started() -> dict[str, Any] | None:
    """Record an RpiCore start, with the time this boot began.

    The boot event's "at" is written before NTP sync and can be stale. boot_at is the current time minus the
    kernel uptime, so it is the true boot time provided the clock is synced when RpiCore starts.
    """
    try:
        event = _new_event(EXPIDITE_STARTED_EVENT)
        event["boot_at"] = _format_time(datetime.now(tz=UTC) - timedelta(seconds=event["uptime_s"]))
        _append_event(event)
    except Exception:
        return None
    else:
        return event


def _new_event(kind: str) -> dict[str, Any]:
    return {
        "at": _format_time(datetime.now(tz=UTC)),
        "boot_id": _read_boot_id(),
        "event": kind,
        "uptime_s": _read_uptime(),
    }


def _read_boot_id() -> str:
    return _BOOT_ID_FILE.read_text().strip()


def _read_uptime() -> int:
    return round(float(_UPTIME_FILE.read_text().split()[0]))


def _format_time(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S")


def _read_power_reset() -> dict[str, Any] | None:
    """Read the Raspberry Pi 5 PMIC reset bitfield exposed through Device Tree."""
    try:
        contents = _POWER_RESET_FILE.read_bytes()
    except OSError:
        # This property is Pi 5-specific, so absence is normal on other models and development machines.
        return None
    if len(contents) != 4:
        return None

    raw = int.from_bytes(contents, byteorder="big")
    return {
        "raw": raw,
        "flags": [name for bit, name in _POWER_RESET_FLAGS.items() if raw & (1 << bit)],
    }


def _rotate_if_full() -> None:
    # The previous backup is deliberately replaced, bounding storage to two small files.
    # A missing active file raises from stat() and is suppressed: there is nothing to rotate.
    with contextlib.suppress(OSError):
        if BOOT_HISTORY_FILE.stat().st_size >= MAX_BYTES_PER_FILE:
            os.replace(BOOT_HISTORY_FILE, BOOT_HISTORY_BACKUP_FILE)


def _append_event(event: dict[str, Any]) -> None:
    with BOOT_HISTORY_FILE.open("a") as f:
        f.write(json.dumps(event) + "\n")
        f.flush()
        os.fsync(f.fileno())


def main() -> None:
    """Command-line entry point used by systemd and the installers."""
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "record-boot" and len(sys.argv) == 2:
        record_boot()
    elif command == "record-shutdown" and len(sys.argv) == 2:
        record_shutdown()
    elif command == "record-request" and len(sys.argv) == 3:
        record_reboot_request(sys.argv[2])
    else:
        usage = "usage: python boot_history.py {record-boot|record-shutdown|record-request <reason>}"
        raise SystemExit(usage)


if __name__ == "__main__":
    main()
