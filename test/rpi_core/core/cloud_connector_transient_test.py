"""Unit tests for transient-network-failure handling in CloudConnector.

A temporary loss of internet on the device surfaces as a network-level Azure error (most commonly a DNS
resolution failure). A brief outage is expected and is retried, so it is logged as a concise warning
rather than a customer-facing fault. A failure that persists across many retries - or any non-transient
error - is escalated to a customer-facing fault, but the stack trace is never attached to the RAISE_WARN
line: it goes to a separate engineer-only log record. See
https://github.com/Oxford-Bee-Ops/expidite/issues/21.
"""

import logging

import pytest
from azure.core.exceptions import ServiceRequestError, ServiceResponseError
from azure.storage.blob import ContainerClient

from expidite_rpi.core import api
from expidite_rpi.core.cloud_connector import cloud_connector as cc_module
from expidite_rpi.core.cloud_connector.cloud_connector import (
    _ESCALATE_AFTER_SECONDS,
    is_transient_network_error,
    log_cloud_failure,
)


def _expidite_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == "expidite"]


def _raise_warn_record(records: list[logging.LogRecord]) -> logging.LogRecord:
    matches = [r for r in records if api.RAISE_WARN_TAG in r.getMessage()]
    assert len(matches) == 1, f"expected exactly one RAISE_WARN record, got {len(matches)}"
    return matches[0]


class TestIsTransientNetworkError:
    @pytest.mark.unittest
    def test_dns_failure_is_transient(self) -> None:
        # Mirrors the issue: "Failed to resolve ... Temporary failure in name resolution".
        exc = ServiceRequestError(message="Failed to resolve 'x.blob.core.windows.net'")
        assert is_transient_network_error(exc)

    @pytest.mark.unittest
    def test_service_response_error_is_transient(self) -> None:
        assert is_transient_network_error(ServiceResponseError(message="server did not respond"))

    @pytest.mark.unittest
    def test_value_error_is_not_transient(self) -> None:
        assert not is_transient_network_error(ValueError("bad config"))


class TestLogCloudFailure:
    @pytest.mark.unittest
    def test_brief_transient_failure_logs_friendly_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """A brief transient failure is a warning with no traceback and no RAISE_WARN tag (not a fault)."""
        exc = ServiceRequestError(message="Failed to resolve 'x.blob.core.windows.net'")
        with caplog.at_level(logging.WARNING, logger="expidite"):
            log_cloud_failure("Failed to append data to V1_HEART_abc.csv", exc, elapsed_seconds=0.0)

        records = _expidite_records(caplog)
        assert len(records) == 1
        record = records[0]
        assert record.levelno == logging.WARNING
        assert api.RAISE_WARN_TAG not in record.getMessage()
        assert record.exc_info is None  # no traceback dumped
        assert "Temporary network failure" in record.getMessage()

    @pytest.mark.unittest
    def test_persistent_transient_failure_escalates_to_fault(self, caplog: pytest.LogCaptureFixture) -> None:
        """Once a transient failure has persisted past the threshold it is escalated to a fault."""
        exc = ServiceRequestError(message="Failed to resolve 'x.blob.core.windows.net'")
        with caplog.at_level(logging.WARNING, logger="expidite"):
            log_cloud_failure(
                "Failed to append data to V1_HEART_abc.csv", exc, elapsed_seconds=_ESCALATE_AFTER_SECONDS
            )

        records = _expidite_records(caplog)
        # One customer-facing fault line + one engineer-facing stack-trace line.
        assert len(records) == 2

        fault = _raise_warn_record(records)
        assert fault.levelno == logging.ERROR
        assert fault.exc_info is None  # never a stack trace alongside RAISE_WARN()
        assert "persistent network failure" in fault.getMessage().lower()
        # The one-line exception message is included on the customer line; only the stack trace is not.
        assert "Failed to resolve" in fault.getMessage()

        trace = next(r for r in records if r is not fault)
        assert trace.levelno == logging.WARNING  # not forwarded to the customer datastream
        assert api.RAISE_WARN_TAG not in trace.getMessage()
        assert trace.exc_info is not None  # full stack trace for engineers

    @pytest.mark.unittest
    def test_non_transient_error_escalates_immediately(self, caplog: pytest.LogCaptureFixture) -> None:
        """A genuine (non-network) error is a fault on the first attempt."""
        exc = ValueError("something is genuinely wrong")
        with caplog.at_level(logging.WARNING, logger="expidite"):
            log_cloud_failure("Failed to append data to V1_HEART_abc.csv", exc, elapsed_seconds=0.0)

        records = _expidite_records(caplog)
        assert len(records) == 2

        fault = _raise_warn_record(records)
        assert fault.levelno == logging.ERROR
        assert fault.exc_info is None

        trace = next(r for r in records if r is not fault)
        assert trace.exc_info is not None
        assert api.RAISE_WARN_TAG not in trace.getMessage()

    @pytest.mark.unittest
    def test_raise_warn_line_never_carries_a_stack_trace(self, caplog: pytest.LogCaptureFixture) -> None:
        """The core invariant: no single log record ever has both RAISE_WARN() and a stack trace."""
        cases = [
            (ServiceRequestError(message="dns down"), 0.0),  # brief transient
            (ServiceRequestError(message="dns down"), _ESCALATE_AFTER_SECONDS),  # persistent transient
            (ValueError("broken"), 0.0),  # non-transient
        ]
        for exc, elapsed_seconds in cases:
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger="expidite"):
                log_cloud_failure(
                    "Failed to append data to V1_HEART_abc.csv", exc, elapsed_seconds=elapsed_seconds
                )
            for record in _expidite_records(caplog):
                if api.RAISE_WARN_TAG in record.getMessage():
                    assert record.exc_info is None, f"RAISE_WARN line had a stack trace for {exc!r}"


class TestAppendDataToBlobEscalation:
    @staticmethod
    def _connector_that_fails_with(exc: Exception) -> cc_module.CloudConnector:
        # Bypass __init__ (needs cloud credentials); stub the one method that touches the network.
        connector = cc_module.CloudConnector.__new__(cc_module.CloudConnector)

        def _boom(dst_container: str) -> ContainerClient:
            del dst_container  # unused; the stub always fails
            raise exc

        connector._validate_container = _boom  # type: ignore[method-assign, assignment]
        return connector

    @pytest.mark.unittest
    def test_brief_transient_error_returns_false_without_fault(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A DNS failure on an early attempt returns False and logs a friendly warning, not a fault.

        This reproduces the exact path from the issue, where blob_client.exists() raised
        ServiceRequestError and the whole traceback was logged with the RAISE_WARN tag.
        """
        connector = self._connector_that_fails_with(
            ServiceRequestError(message="Failed to resolve blob host")
        )
        with caplog.at_level(logging.WARNING, logger="expidite"):
            result = connector._append_data_to_blob(
                dst_container="expidite-upload",
                dst_file="V1_HEART_abc.csv",
                lines_to_append=["col1,col2\n", "1,2\n"],
                elapsed_seconds=0.0,
            )

        assert result is False
        records = _expidite_records(caplog)
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert api.RAISE_WARN_TAG not in records[0].getMessage()

    @pytest.mark.unittest
    def test_persistent_transient_error_escalates(self, caplog: pytest.LogCaptureFixture) -> None:
        """The elapsed time plumbed in from the async layer drives escalation inside _append_data_to_blob."""
        connector = self._connector_that_fails_with(
            ServiceRequestError(message="Failed to resolve blob host")
        )
        with caplog.at_level(logging.WARNING, logger="expidite"):
            result = connector._append_data_to_blob(
                dst_container="expidite-upload",
                dst_file="V1_HEART_abc.csv",
                lines_to_append=["col1,col2\n", "1,2\n"],
                elapsed_seconds=_ESCALATE_AFTER_SECONDS,
            )

        assert result is False
        records = _expidite_records(caplog)
        fault = _raise_warn_record(records)
        assert fault.exc_info is None
