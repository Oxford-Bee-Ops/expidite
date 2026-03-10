from types import TracebackType
from typing import Literal, cast

import pytest

from expidite_rpi.sensors import sensor_sht20


class _WaitRecorder:
    def __init__(self) -> None:
        self.wait_calls: list[float] = []

    def wait(self, seconds: float) -> None:
        self.wait_calls.append(seconds)


class _ResponseValue:
    def __init__(self, ticks: float) -> None:
        self.ticks = ticks
        self.degrees_celsius = ticks
        self.percent_rh = ticks


class _DummySHT20Self:
    def __init__(self) -> None:
        self.sensor_index = 64
        self.stop_requested = _WaitRecorder()
        self.logged_data: list[dict[str, str]] = []
        self._continue_values = iter([True, True, True, True, False, False])

    def continue_recording(self) -> bool:
        return next(self._continue_values)

    def in_review_mode(self) -> bool:
        return False

    def log(self, stream_index: int, sensor_data: dict[str, str]) -> None:
        self.logged_data.append(sensor_data)


@pytest.mark.unittest
def test_sht20_startup_retries_after_transient_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    startup_attempts = {"count": 0}

    class FakeTransceiver:
        def __init__(self, _device: str) -> None:
            pass

        def __enter__(self) -> "FakeTransceiver":
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ) -> Literal[False]:
            return False

    class FakeSht2xDevice:
        def __init__(self, _connection: object, slave_address: int = 0x40) -> None:
            _ = slave_address

        def soft_reset(self) -> None:
            startup_attempts["count"] += 1
            if startup_attempts["count"] < 3:
                raise OSError(121, "Remote I/O error")

        def read_serial_number(self) -> str:
            return "fake-serial"

        def single_shot_measurement(self) -> tuple[_ResponseValue, _ResponseValue]:
            return (_ResponseValue(20.5), _ResponseValue(50.1))

    monkeypatch.setattr(sensor_sht20, "LinuxI2cTransceiver", FakeTransceiver)
    monkeypatch.setattr(sensor_sht20, "I2cConnection", lambda transceiver: transceiver)
    monkeypatch.setattr(sensor_sht20, "Sht2xI2cDevice", FakeSht2xDevice)

    dummy_sensor = _DummySHT20Self()
    sensor_sht20.SHT20.run(cast(sensor_sht20.SHT20, dummy_sensor))

    assert startup_attempts["count"] == 3
    assert len(dummy_sensor.logged_data) == 1
    assert dummy_sensor.logged_data[0]["temperature"] == "20.5"
    assert dummy_sensor.logged_data[0]["humidity"] == "50.1"
