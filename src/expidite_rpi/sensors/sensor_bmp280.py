from dataclasses import dataclass

import adafruit_bmp280
from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg

try:
    # This is only needed for typing
    import board  # type: ignore
except (ImportError, NotImplementedError):
    # Running on non-CircuitPython environment (Windows/standard Python)
    board = None
    pass

logger = root_cfg.setup_logger("expidite")

BMP280_STREAM_INDEX = 0
BMP280_SENSOR_INDEX = 118 # BMP280 i2c address, 0x76(118)
BMP280_SENSOR_TYPE_ID = "BMP280"
BMP280_FIELDS = ["pressure"]

@dataclass
class BMP280SensorCfg(SensorCfg):
    ############################################################
    # SensorCfg fields
    ############################################################
    sensor_type: api.SENSOR_TYPE = api.SENSOR_TYPE.I2C
    sensor_index: int = BMP280_SENSOR_INDEX
    sensor_model: str = "BMP280"
    description: str = "BMP280 Temperature and Humidity sensor"

DEFAULT_BMP280_SENSOR_CFG = BMP280SensorCfg(
    outputs=[
        Stream(
            description="Temperature and humidity data from BMP280",
            type_id=BMP280_SENSOR_TYPE_ID,
            index=BMP280_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=BMP280_FIELDS,
            cloud_container="expidite-journals",
        )
    ],
)


class BMP280(Sensor):
    # Init
    def __init__(self, config: BMP280SensorCfg):
        super().__init__(config)
        self.config = config
        
    # Separate thread to log data
    def run(self):

        i2c = board.I2C()
        sensor = adafruit_bmp280.Adafruit_BMP280_I2C(i2c)

        while self.continue_recording():
            try:
                pressure = sensor.pressure

                if pressure is None:
                    logger.error(f"{root_cfg.RAISE_WARN()}Error in BMP280 sensor run: No data")
                    continue

                self.log(
                    stream_index=BMP280_STREAM_INDEX,
                    sensor_data={"pressure": ("%.1f" % pressure)},
                )
                logger.debug(f"BMP280 sensor {self.sensor_index} data: "
                                f"{pressure:.1f}Pa")

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in BMP280 sensor run: {e}", exc_info=True)
            finally:
                logger.debug(f"BMP280 sensor {self.sensor_index} sleeping for "
                            f"{root_cfg.my_device.env_sensor_frequency} seconds")
                self.stop_requested.wait(root_cfg.my_device.env_sensor_frequency)

