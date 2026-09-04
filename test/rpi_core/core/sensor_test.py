"""Tests for the Sensor recording-loop control in core/sensor.py."""

from unittest.mock import patch

import pytest

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import SensorCfg
from expidite_rpi.core.sensor import Sensor

logger = root_cfg.setup_logger("expidite")

# Bound on how many times a single continue_recording() call may consult failing_to_keep_up(). The loop
# should back off via stop_requested.wait() between checks, so a healthy implementation needs very few.
# The cap exists so that a regression to a spin loop fails the test rather than hanging the suite.
MAX_KEEP_UP_CHECKS = 20


class _TestSensor(Sensor):
    """Minimal concrete Sensor; continue_recording() is inherited and needs no sensing implementation."""

    __test__ = False  # Stop pytest collecting this as a test class.


def _make_sensor() -> _TestSensor:
    return _TestSensor(
        SensorCfg(
            outputs=[],
            description="Sensor used to exercise continue_recording",
            sensor_type=api.SENSOR_TYPE.I2C,
            sensor_index=0,
            sensor_model="TestSensor",
        )
    )


class Test_continue_recording:
    @pytest.mark.unittest
    def test_returns_true_when_healthy(self) -> None:
        """The normal path: not stopping, keeping up, so recording continues without backing off."""
        sensor = _make_sensor()

        with (
            patch("expidite_rpi.utils.utils.failing_to_keep_up", return_value=False),
            patch.object(sensor.stop_requested, "wait") as mock_wait,
        ):
            assert sensor.continue_recording() is True

        assert mock_wait.call_count == 0, "Should not back off when the system is keeping up"

    @pytest.mark.unittest
    def test_returns_false_when_stop_requested(self) -> None:
        """A stop request ends the recording cycle even when the system is keeping up fine."""
        sensor = _make_sensor()
        sensor.stop()

        with patch("expidite_rpi.utils.utils.failing_to_keep_up", return_value=False):
            assert sensor.continue_recording() is False

    @pytest.mark.unittest
    def test_holds_up_thread_until_backlog_clears(self) -> None:
        """While we are failing to keep up, the thread is held up, then resumes once the backlog drains."""
        sensor = _make_sensor()
        outcomes = iter([True, True, False])

        with (
            patch("expidite_rpi.utils.utils.failing_to_keep_up", side_effect=lambda: next(outcomes)),
            patch.object(sensor.stop_requested, "wait") as mock_wait,
        ):
            assert sensor.continue_recording() is True

        assert mock_wait.call_count == 2, "Should back off once per failing_to_keep_up() check"
        # The back-off is the full recording timer, so a stalled device rechecks rarely rather than busily.
        for call in mock_wait.call_args_list:
            assert call[0][0] == root_cfg.my_device.max_recording_timer

    @pytest.mark.unittest
    def test_stop_request_breaks_out_of_backlog_wait(self) -> None:
        """Regression: a stop requested while the mount is full must not spin the sensor thread.

        Event.wait() returns immediately once the event is set, so a loop that only tests
        failing_to_keep_up() burns a core per sensor thread for as long as the mount stays full - starving
        the DP workers that would drain it, and overrunning the shutdown budget in reboot.py.
        """
        sensor = _make_sensor()
        sensor.stop()
        checks = 0

        def never_keeping_up() -> bool:
            nonlocal checks
            checks += 1
            assert checks <= MAX_KEEP_UP_CHECKS, "continue_recording() is spinning instead of returning"
            return True

        # Note the real Event is left unpatched: wait() returns instantly because stop() set it, which is
        # exactly the condition that produced the spin.
        with patch("expidite_rpi.utils.utils.failing_to_keep_up", side_effect=never_keeping_up):
            assert sensor.continue_recording() is False

        assert checks <= MAX_KEEP_UP_CHECKS
