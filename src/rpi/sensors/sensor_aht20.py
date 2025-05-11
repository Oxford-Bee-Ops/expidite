from dataclasses import dataclass

from sensor_core import Sensor, SensorCfg, api
from sensor_core import configuration as root_cfg
from sensor_core.dp_config_objects import Stream
from sensor_core.sensors.drivers.aht20 import AHT20 as AHT20_driver

logger = root_cfg.setup_logger("sensor_core")

AHT20_STREAM_INDEX = 0
AHT20_SENSOR_INDEX = 56 # AHT20 i2c address, 0x38 (56)
AHT20_SENSOR_TYPE_ID = "AHT20"
AHT20_FIELDS = ["temperature", "humidity"]

@dataclass
class AHT20SensorCfg(SensorCfg):
    ############################################################
    # SensorCfg fields
    ############################################################
    # The type of sensor.
    sensor_type: api.SENSOR_TYPE = api.SENSOR_TYPE.I2C
    sensor_index: int = AHT20_SENSOR_INDEX
    sensor_model: str = "AHT20"
    # A human-readable description of the sensor model.
    description: str = "AHT20 Temperature and Humidity sensor"

    ############################################################
    # Custom fields
    ############################################################

DEFAULT_AHT20_SENSOR_CFG = AHT20SensorCfg(
    outputs=[
        Stream(
            description="Temperature and humidity data from AHT20",
            type_id=AHT20_SENSOR_TYPE_ID,
            index=AHT20_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=AHT20_FIELDS,
            cloud_container="sensor-core-journals",
        )
    ],
)

class AHT20(Sensor):
    # Init
    def __init__(self, config: AHT20SensorCfg):
        super().__init__(config)
        self.config = config
        
    # Separate thread to log data
    def run(self):

        while not self.stop_requested.is_set():
            try:
                aht20 = AHT20_driver(1)
                temperature = aht20.get_temperature()
                humidity = aht20.get_humidity()

                if temperature is None or humidity is None:
                    logger.error(f"{root_cfg.RAISE_WARN()}Error in AHT20 sensor run: No data")
                    continue

                self.log(
                    stream_index=AHT20_STREAM_INDEX,
                    sensor_data={"temperature": ("%.1f" % temperature),
                                 "humidity": ("%.1f" % humidity)},
                )
 
            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in AHT20 sensor run: {e}", exc_info=True)
            finally:
                logger.debug(f"AHT20 sensor {self.sensor_index} sleeping for "
                             f"{root_cfg.my_device.env_sensor_frequency} seconds")
                self.stop_requested.wait(root_cfg.my_device.env_sensor_frequency)

