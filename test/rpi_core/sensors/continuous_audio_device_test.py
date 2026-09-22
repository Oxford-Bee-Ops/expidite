from datetime import datetime
from pathlib import Path
from typing import cast

import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.sensors import sensor_continuous_audio
from expidite_rpi.sensors.sensor_continuous_audio import (
    AUDIO_SENSOR_STREAM_INDEX,
    DEFAULT_CONTINUOUS_AUDIO_SENSOR_CFG,
    ContinuousAudioSensor,
    ContinuousAudioSensorCfg,
)

root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING


class _WaitRecorder:
    def __init__(self) -> None:
        self.wait_calls: list[float] = []

    def wait(self, seconds: float) -> None:
        self.wait_calls.append(seconds)


class _DummyContinuousAudioSelf:
    """Stand-in for ContinuousAudioSensor that lets us call run() without real hardware/threads."""

    def __init__(self, config: ContinuousAudioSensorCfg, num_chunks: int) -> None:
        self.config = config
        self.stop_requested = _WaitRecorder()
        self.saved_recordings: list[dict[str, object]] = []
        self._continue_values = iter([True] * num_chunks + [False])

    def continue_recording(self) -> bool:
        return next(self._continue_values)

    def sensor_failed(self) -> None:
        raise AssertionError("sensor_failed should not be called in this test")

    def save_recording(
        self,
        stream_index: int,
        temporary_file: Path,
        start_time: datetime,
        end_time: datetime | None = None,
        override_sampling: object = None,
        can_discard: bool = False,
    ) -> Path:
        self.saved_recordings.append(
            {
                "stream_index": stream_index,
                "temporary_file": temporary_file,
                "start_time": start_time,
                "end_time": end_time,
            }
        )
        return temporary_file


@pytest.mark.unittest
def test_continuous_audio_records_chunks_of_max_recording_timer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(root_cfg.my_device, "max_recording_timer", 3)

    run_commands: list[str] = []

    def fake_run_cmd(cmd: str, *args: object, **kwargs: object) -> str:
        run_commands.append(cmd)
        if cmd.startswith("arecord -l"):
            return "card 1: Device [USB Audio Device], device 0: USB Audio [USB Audio]"
        return "arecord command emulated successfully"

    monkeypatch.setattr(sensor_continuous_audio.utils, "run_cmd", fake_run_cmd)

    dummy_sensor = _DummyContinuousAudioSelf(DEFAULT_CONTINUOUS_AUDIO_SENSOR_CFG, num_chunks=2)
    ContinuousAudioSensor.run(cast(ContinuousAudioSensor, dummy_sensor))

    # continue_recording() returned True twice, so we expect 2 recorded chunks
    assert len(dummy_sensor.saved_recordings) == 2
    for recording in dummy_sensor.saved_recordings:
        assert recording["stream_index"] == AUDIO_SENSOR_STREAM_INDEX

    # Each chunk should record for max_recording_timer seconds against the discovered USB device
    record_cmds = [cmd for cmd in run_commands if cmd.startswith("arecord -D")]
    assert len(record_cmds) == 2
    for cmd in record_cmds:
        assert " -d 3 " in cmd
        assert "hw:1,0" in cmd
