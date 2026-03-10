from collections.abc import Callable
from types import TracebackType
from typing import Literal, cast

import pytest

from expidite_rpi.sensors import sensor_sht20
from expidite_rpi.sensors.drivers import aht20 as aht20_driver


class _WaitRecorder:
    def __init__(self) -> None:
        self.wait_calls: list[float] = []

    def wait(self, seconds: float) -> None:
        self.wait_calls.append(seconds)


class _ResponseValue:
    def __init__(self, ticks: float) -> None:
        self.ticks = ticks


def _fake_i2c_channel(connection: object, slave_address: int, crc: object) -> object:
    _ = connection
    _ = slave_address
    _ = crc
    return object()


class _DummySHT20Self:
    def __init__(self) -> None:
        self.sensor_index = 64
        self.stop_requested = _WaitRecorder()
        self.logged_data: list[dict[str, str]] = []
        self._continue_values = iter([True, True, False, False])

    def continue_recording(self) -> bool:
        return next(self._continue_values)

    def in_review_mode(self) -> bool:
        return False

    def log(self, stream_index: int, sensor_data: dict[str, str]) -> None:
        self.logged_data.append(sensor_data)


@pytest.mark.unittest
def test_aht20_soft_reset_retries_on_transient_i2c_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}

    class FakeSMBus:
        def __init__(self, bus_num: int) -> None:
            self.bus_num = bus_num

        def __enter__(self) -> "FakeSMBus":
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ) -> Literal[False]:
            return False

        def write_i2c_block_data(self, *args: object, **kwargs: object) -> None:
            _ = args
            _ = kwargs
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise OSError(121, "Remote I/O error")

    monkeypatch.setattr(aht20_driver, "SMBus", FakeSMBus)
    monkeypatch.setattr(aht20_driver.time, "sleep", lambda _seconds: None)

    driver = aht20_driver.AHT20.__new__(aht20_driver.AHT20)
    driver.BusNum = 1
    cmd_soft_reset = cast(Callable[[], bool], driver.cmd_soft_reset)

    assert cmd_soft_reset() is True
    assert attempts["count"] == 3


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
        def __init__(self, _channel: object) -> None:
            pass

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
    monkeypatch.setattr(sensor_sht20, "I2cChannel", _fake_i2c_channel)
    monkeypatch.setattr(sensor_sht20, "CrcCalculator", lambda *_args: object())
    monkeypatch.setattr(sensor_sht20, "Sht2xI2cDevice", FakeSht2xDevice)
    monkeypatch.setattr(sensor_sht20, "sleep", lambda _seconds: None)

    dummy_sensor = _DummySHT20Self()
    sensor_sht20.SHT20.run(cast(sensor_sht20.SHT20, dummy_sensor))

    assert startup_attempts["count"] == 3
    assert len(dummy_sensor.logged_data) == 1
    assert dummy_sensor.logged_data[0]["temperature"] == "20.5"
    assert dummy_sensor.logged_data[0]["humidity"] == "50.1"
