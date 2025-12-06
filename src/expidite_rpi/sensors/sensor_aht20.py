from dataclasses import dataclass

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from expidite_rpi.sensors.drivers.aht20 import AHT20 as AHT20_driver

logger = root_cfg.setup_logger("expidite")

AHT20_SENSOR_INDEX = 56  # AHT20 i2c address, 0x38 (56)
AHT20_SENSOR_TYPE_ID = "AHT20"
AHT20_FIELDS = ["temperature", "humidity"]
AHT20_STREAM_INDEX = 0
AHT20_STREAM: Stream = Stream(
    description="Temperature and humidity data from AHT20",
    type_id=AHT20_SENSOR_TYPE_ID,
    index=AHT20_STREAM_INDEX,
    format=api.FORMAT.LOG,
    fields=AHT20_FIELDS,
    cloud_container="expidite-journals",
)


@dataclass
class AHT20SensorCfg(SensorCfg):
    ##########################################################################################################
    # Custom fields
    ##########################################################################################################
    pass


DEFAULT_AHT20_SENSOR_CFG = AHT20SensorCfg(
    sensor_type=api.SENSOR_TYPE.I2C,
    sensor_index=AHT20_SENSOR_INDEX,
    sensor_model="AHT20",
    description="AHT20 Temperature and Humidity sensor",
    outputs=[AHT20_STREAM],
)


class AHT20(Sensor):
    # Init
    def __init__(self, config: AHT20SensorCfg) -> None:
        super().__init__(config)
        self.config = config

    # Separate thread to log data
    def run(self) -> None:
        while self.continue_recording():
            try:
                aht20 = AHT20_driver(1)
                temperature = aht20.get_temperature()
                humidity = aht20.get_humidity()

                self.log(
                    stream_index=AHT20_STREAM_INDEX,
                    sensor_data={"temperature": ("%.1f" % temperature), "humidity": ("%.1f" % humidity)},
                )

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in AHT20 sensor run: {e}", exc_info=True)
            finally:
                logger.debug(
                    f"AHT20 sensor {self.sensor_index} sleeping for "
                    f"{root_cfg.my_device.env_sensor_frequency} seconds"
                )

                if self.in_review_mode():
                    wait_period = root_cfg.my_device.review_mode_frequency
                else:
                    wait_period = root_cfg.my_device.env_sensor_frequency
                self.stop_requested.wait(wait_period)
