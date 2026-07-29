from datetime import UTC, datetime
from typing import Any

import pytest

from expidite_rpi.core import api, device_health
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_health import WARNING_STREAM_INDEX, DeviceHealth

logger = root_cfg.setup_logger("expidite")
root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING

# journald's MESSAGE field holds the *formatted* record, so setup_logger's formatter prefixes every message
# with "<name> <LEVEL> [<thread>] ". Tests must reproduce that prefix or they will not exercise the real
# input to log_warnings().
_JOURNAL_PREFIX = "bee_ops WARNING [140734573938656] "


def _entry(message: str, priority: int) -> dict[str, Any]:
    """Build a journal entry in the shape get_logs() returns."""
    return {
        "time_logged": datetime(2026, 7, 29, 17, 48, 29, tzinfo=UTC),
        "message": message,
        "process_id": 3125,
        "process_name": "run_etl_pipeline.py",
        "executable_path": "/usr/bin/python3",
        "priority": priority,
    }


class Test_log_warnings:
    def _capture(
        self, monkeypatch: pytest.MonkeyPatch, entries: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Run log_warnings() over `entries` and return the rows it forwarded to the WARNING stream."""
        captured: list[dict[str, Any]] = []

        def fake_log(stream_index: int, sensor_data: dict[str, Any]) -> None:
            assert stream_index == WARNING_STREAM_INDEX
            captured.append(sensor_data)

        monkeypatch.setattr(root_cfg, "running_on_rpi", True)
        monkeypatch.setattr(device_health, "get_logs", lambda **_kwargs: entries)
        health = DeviceHealth()
        monkeypatch.setattr(health, "log", fake_log)
        health.log_warnings()
        return captured

    @pytest.mark.unittest
    def test_tagged_warning_is_captured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A RAISE_WARN logged at WARNING level must reach the WARNING datastream.

        Regression test: the tag sits mid-message behind the formatter prefix, so anchoring the match at the
        start of the message dropped every warning-level fault (priority 4 also fails the <=3 fallback).
        """
        logger.info("Run test_tagged_warning_is_captured test")
        message = (
            f"{_JOURNAL_PREFIX}{api.RAISE_WARN_TAG}_2ccf6791818a: \n"
            "ETL Helper Error Report for agrocare001:\n"
            "- Environmental data unavailable; HIVE-ENRICHED output will have empty env columns"
        )
        captured = self._capture(monkeypatch, [_entry(message, priority=4)])

        assert len(captured) == 1
        assert captured[0]["message"] == message
        # Recorded as journald reported it - the tag decides capture, it does not change the priority.
        assert captured[0]["priority"] == 4

    @pytest.mark.unittest
    def test_tagged_error_priority_is_unchanged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A tagged ERROR is captured on priority alone, and is not escalated by the tag."""
        logger.info("Run test_tagged_error_priority_is_unchanged test")
        message = f"{_JOURNAL_PREFIX}{api.RAISE_WARN_TAG}_2ccf6791818a: Failed to get telemetry"
        captured = self._capture(monkeypatch, [_entry(message, priority=3)])

        assert len(captured) == 1
        assert captured[0]["priority"] == 3

    @pytest.mark.unittest
    def test_tagged_info_is_not_captured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The tag only promotes WARNING-level records; below that, priority still governs."""
        logger.info("Run test_tagged_info_is_not_captured test")
        message = f"{_JOURNAL_PREFIX}{api.RAISE_WARN_TAG}_2ccf6791818a: fyi"
        captured = self._capture(monkeypatch, [_entry(message, priority=6)])

        assert captured == []

    @pytest.mark.unittest
    def test_untagged_records_follow_priority(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Untagged records are captured on priority alone (<=3) and keep their priority unchanged."""
        logger.info("Run test_untagged_records_follow_priority test")
        captured = self._capture(
            monkeypatch,
            [
                _entry(f"{_JOURNAL_PREFIX}some unrelated error", priority=3),
                _entry(f"{_JOURNAL_PREFIX}camera settled slowly", priority=4),
                _entry(f"{_JOURNAL_PREFIX}Combining processed chunks...", priority=6),
            ],
        )

        assert len(captured) == 1
        assert captured[0]["message"].endswith("some unrelated error")
        assert captured[0]["priority"] == 3
