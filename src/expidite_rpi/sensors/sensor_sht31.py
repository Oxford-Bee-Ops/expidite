from dataclasses import dataclass
from time import sleep
from typing import ClassVar

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg

logger = root_cfg.setup_logger("expidite")

SHT31_SENSOR_INDEX = 68  # SHT31 i2c address, 0x44(68)
SHT31_SENSOR_TYPE_ID = "SHT31"
SHT31_FIELDS = ["temperature", "humidity"]
SHT31_STREAM_INDEX = 0
SHT31_STREAM: Stream = Stream(
    description="Temperature and humidity data from SHT31",
    type_id=SHT31_SENSOR_TYPE_ID,
    index=SHT31_STREAM_INDEX,
    format=api.FORMAT.LOG,
    fields=SHT31_FIELDS,
    cloud_container="expidite-journals",
)


@dataclass
class SHT31SensorCfg(SensorCfg):
    ############################################################
    # Custom fields
    ############################################################
    pass


DEFAULT_SHT31_SENSOR_CFG = SHT31SensorCfg(
    sensor_type=api.SENSOR_TYPE.I2C,
    sensor_index=SHT31_SENSOR_INDEX,
    sensor_model="SHT31",
    description="SHT31 Temperature and Humidity sensor",
    outputs=[SHT31_STREAM],
)


@dataclass
class SHT31_CFG:
    address = 0x44
    write_register = 0x2C
    write_data: ClassVar = [0x06]
    read_register = 0x00
    read_length = 6
    read_delay = 0.25


class SHT31(Sensor):
    # Init
    def __init__(self, config: SHT31SensorCfg) -> None:
        super().__init__(config)
        self.config = config

    def read_data(self) -> tuple[float, float]:
        cTemp: float
        humidity: float

        if root_cfg.running_on_linux:
            import smbus2 as smbus

            with smbus.SMBus(1) as bus:
                # SHT31 address, 0x44(68)
                bus.write_i2c_block_data(SHT31_CFG.address, SHT31_CFG.write_register, SHT31_CFG.write_data)
                sleep(SHT31_CFG.read_delay)

                # Read data back from 0x00(00), 6 bytes
                # Temp MSB, Temp LSB, Temp CRC, Humidity MSB, Humidity LSB, Humidity CRC
                data = bus.read_i2c_block_data(
                    SHT31_CFG.address, SHT31_CFG.read_register, SHT31_CFG.read_length
                )

                # Convert the data
                temp = data[0] * 256 + data[1]
                cTemp = -45 + (175 * temp / 65535.0)
                humidity = 100 * (data[3] * 256 + data[4]) / 65535.0

        else:
            # Test mode on windows
            assert root_cfg.ST_MODE == root_cfg.SOFTWARE_TEST_MODE.TESTING, "Test mode not set"
            cTemp = 25.0
            humidity = 50.0

        return cTemp, humidity

    # Separate thread to log data
    def run(self) -> None:
        while self.continue_recording():
            try:
                temperature, humidity = self.read_data()

                self.log(
                    stream_index=SHT31_STREAM_INDEX,
                    sensor_data={"temperature": ("%.1f" % temperature), "humidity": ("%.1f" % humidity)},
                )
                logger.debug(f"SHT31 sensor {self.sensor_index} data: {temperature:.1f}C, {humidity:.1f}%")

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in SHT31 sensor run: {e}", exc_info=True)
            finally:
                if self.in_review_mode():
                    wait_period = root_cfg.my_device.review_mode_frequency
                else:
                    wait_period = root_cfg.my_device.env_sensor_frequency
                logger.debug(f"SHT31 sensor {self.sensor_index} sleeping for {wait_period} seconds")
                self.stop_requested.wait(wait_period)
