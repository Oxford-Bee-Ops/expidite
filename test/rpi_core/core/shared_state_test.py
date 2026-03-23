import time

import pytest

from expidite_rpi.core import api
from expidite_rpi.core.dp_config_objects import SensorCfg, Stream
from expidite_rpi.core.sensor import Sensor
from expidite_rpi.core.shared_state import SharedState


class _TestSensor(Sensor):
    def run(self) -> None:
        return


class TestSharedState:
    @pytest.mark.unittest
    def test_set_get_and_version(self) -> None:
        state = SharedState.get_instance()
        state.clear()

        version_1 = state.set("sensor.camera.threshold", 0.7)
        version_2 = state.set("sensor.camera.threshold", 0.8)

        assert version_1 == 1
        assert version_2 == 2
        assert state.get("sensor.camera.threshold") == 0.8

    @pytest.mark.unittest
    def test_ttl_expiry(self) -> None:
        state = SharedState.get_instance()
        state.clear()

        state.set("sensor.camera.mode", "review", ttl_seconds=0.1)
        assert state.get("sensor.camera.mode") == "review"

        time.sleep(0.2)
        assert state.get("sensor.camera.mode") is None

    @pytest.mark.unittest
    def test_sensor_wrapper_methods(self) -> None:
        SharedState.get_instance().clear()
        cfg = SensorCfg(
            sensor_type=api.SENSOR_TYPE.I2C,
            sensor_index=99,
            sensor_model="TestSensor",
            description="Shared state test sensor",
            outputs=[
                Stream(
                    description="test stream",
                    type_id="TESTS",
                    index=0,
                    format=api.FORMAT.LOG,
                    fields=["v"],
                )
            ],
        )
        sensor = _TestSensor(cfg)

        sensor.shared_set("sensor.test.value", {"a": 1})
        assert sensor.shared_get("sensor.test.value") == {"a": 1}

        keys = sensor.shared_list_keys(prefix="sensor.test")
        assert keys == ["sensor.test.value"]

        assert sensor.shared_delete("sensor.test.value") is True
        assert sensor.shared_get("sensor.test.value") is None
