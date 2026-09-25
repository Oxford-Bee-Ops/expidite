import contextlib
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from expidite_rpi.scripts import boot_history


@pytest.fixture
def diags_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(boot_history, "DIAGS_DIR", tmp_path)
    monkeypatch.setattr(boot_history, "BOOT_HISTORY_FILE", tmp_path / "boot_history.jsonl")
    monkeypatch.setattr(boot_history, "BOOT_HISTORY_BACKUP_FILE", tmp_path / "boot_history.1.jsonl")
    monkeypatch.setattr(boot_history, "_POWER_RESET_FILE", tmp_path / "power_reset")
    _set_uptime(tmp_path, 12.34)
    monkeypatch.setattr(boot_history, "_UPTIME_FILE", tmp_path / "uptime")
    return tmp_path


def _set_uptime(diags_dir: Path, uptime_s: float) -> None:
    (diags_dir / "uptime").write_text(f"{uptime_s:.2f} 40.00\n")


def _set_boot_id(monkeypatch: pytest.MonkeyPatch, boot_id: str) -> None:
    monkeypatch.setattr(boot_history, "_read_boot_id", lambda: boot_id)


def _events(diags_dir: Path) -> list[dict[str, Any]]:
    path = diags_dir / "boot_history.jsonl"
    if not path.exists():
        return []
    events = []
    for line in path.read_text().splitlines():
        with contextlib.suppress(json.JSONDecodeError):
            events.append(json.loads(line))
    return events


class TestRecordBoot:
    @pytest.mark.unittest
    def test_boot_event_contains_only_evidence(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")

        event = boot_history.record_boot()

        assert event is not None
        assert list(event) == ["at", "boot_id", "event", "uptime_s"]
        assert event["event"] == boot_history.BOOT_EVENT
        assert event["boot_id"] == "boot-a"
        assert event["uptime_s"] == 12
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", event["at"])
        assert _events(diags_dir) == [event]

    @pytest.mark.unittest
    def test_orderly_reboot_records_shutdown_between_boots(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        boot_history.record_boot()
        boot_history.record_shutdown()

        _set_boot_id(monkeypatch, "boot-b")
        boot_history.record_boot()

        assert [(item["event"], item["boot_id"]) for item in _events(diags_dir)] == [
            (boot_history.BOOT_EVENT, "boot-a"),
            (boot_history.SHUTDOWN_EVENT, "boot-a"),
            (boot_history.BOOT_EVENT, "boot-b"),
        ]

    @pytest.mark.unittest
    def test_unexpected_reset_leaves_consecutive_boots(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        boot_history.record_boot()

        _set_boot_id(monkeypatch, "boot-b")
        boot_history.record_boot()

        assert [(item["event"], item["boot_id"]) for item in _events(diags_dir)] == [
            (boot_history.BOOT_EVENT, "boot-a"),
            (boot_history.BOOT_EVENT, "boot-b"),
        ]

    @pytest.mark.unittest
    def test_pi5_power_reset_is_recorded(self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        boot_history._POWER_RESET_FILE.write_bytes((0b1_0010).to_bytes(4, byteorder="big"))

        event = boot_history.record_boot()

        assert event is not None
        assert event["power_reset"] == {
            "raw": 0b1_0010,
            "flags": ["under_voltage", "watchdog"],
        }

    @pytest.mark.unittest
    def test_non_pi5_boot_omits_power_reset(self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_boot_id(monkeypatch, "boot-a")

        event = boot_history.record_boot()

        assert event is not None
        assert "power_reset" not in event

    @pytest.mark.unittest
    def test_malformed_power_reset_is_ignored(self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        boot_history._POWER_RESET_FILE.write_bytes(b"bad")

        event = boot_history.record_boot()

        assert event is not None
        assert "power_reset" not in event

    @pytest.mark.unittest
    def test_full_history_rotates_before_recording_a_boot(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Any existing content counts as full, so every boot after the first rotates.
        monkeypatch.setattr(boot_history, "MAX_BYTES_PER_FILE", 1)
        _set_boot_id(monkeypatch, "boot-a")
        boot_history.record_boot()
        boot_history.record_shutdown()

        _set_boot_id(monkeypatch, "boot-b")
        event = boot_history.record_boot()

        assert event is not None
        assert [item["boot_id"] for item in _events(diags_dir)] == ["boot-b"]
        backup_lines = boot_history.BOOT_HISTORY_BACKUP_FILE.read_text().splitlines()
        assert len(backup_lines) == 2

        boot_history.record_shutdown()

        assert [item["event"] for item in _events(diags_dir)] == [
            boot_history.BOOT_EVENT,
            boot_history.SHUTDOWN_EVENT,
        ]
        assert boot_history.BOOT_HISTORY_BACKUP_FILE.read_text().splitlines() == backup_lines

    @pytest.mark.unittest
    def test_write_failure_does_not_break_startup(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        monkeypatch.setattr(
            boot_history,
            "_append_event",
            lambda _event: (_ for _ in ()).throw(OSError("read-only filesystem")),
        )

        assert boot_history.record_boot() is None


class TestRecordShutdown:
    @pytest.mark.unittest
    def test_shutdown_event_contains_only_evidence(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")

        event = boot_history.record_shutdown()

        assert event is not None
        assert list(event) == ["at", "boot_id", "event", "uptime_s"]
        assert event["event"] == boot_history.SHUTDOWN_EVENT
        assert _events(diags_dir) == [event]

    @pytest.mark.unittest
    def test_write_failure_does_not_break_shutdown(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        monkeypatch.setattr(
            boot_history,
            "_append_event",
            lambda _event: (_ for _ in ()).throw(OSError("read-only filesystem")),
        )

        assert boot_history.record_shutdown() is None


class TestRecordRebootRequest:
    @pytest.mark.unittest
    def test_requested_reboot_is_recorded_before_shutdown(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        boot_history.record_boot()
        boot_history.record_reboot_request("managed reboot: wifi outage")
        boot_history.record_shutdown()

        events = _events(diags_dir)

        assert [(item["event"], item["boot_id"]) for item in events] == [
            (boot_history.BOOT_EVENT, "boot-a"),
            (boot_history.REBOOT_REQUESTED_EVENT, "boot-a"),
            (boot_history.SHUTDOWN_EVENT, "boot-a"),
        ]
        assert events[1]["reason"] == "managed reboot: wifi outage"

    @pytest.mark.unittest
    def test_reason_is_bounded(self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_boot_id(monkeypatch, "boot-a")

        event = boot_history.record_reboot_request("x" * 5000)

        assert event is not None
        assert event["reason"] == "x" * boot_history.MAX_REASON_CHARS

    @pytest.mark.unittest
    def test_write_failure_does_not_block_reboot(
        self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        monkeypatch.setattr(
            boot_history,
            "_append_event",
            lambda _event: (_ for _ in ()).throw(OSError("read-only filesystem")),
        )

        assert boot_history.record_reboot_request("managed reboot") is None


class TestRecordExpiditeStarted:
    @pytest.mark.unittest
    def test_boot_time_is_now_minus_uptime(self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        _set_uptime(diags_dir, 100.0)

        before = datetime.now(tz=UTC).replace(microsecond=0)
        event = boot_history.record_expidite_started()
        after = datetime.now(tz=UTC)

        assert event is not None
        assert list(event) == ["at", "boot_id", "event", "uptime_s", "boot_at"]
        assert event["event"] == boot_history.EXPIDITE_STARTED_EVENT
        boot_at = datetime.strptime(event["boot_at"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
        assert before - timedelta(seconds=101) <= boot_at <= after - timedelta(seconds=100)
        assert _events(diags_dir) == [event]

    @pytest.mark.unittest
    def test_write_failure_is_silent(self, diags_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_boot_id(monkeypatch, "boot-a")
        monkeypatch.setattr(
            boot_history,
            "_append_event",
            lambda _event: (_ for _ in ()).throw(OSError("read-only filesystem")),
        )

        assert boot_history.record_expidite_started() is None
