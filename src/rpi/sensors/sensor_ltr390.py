##########################################################################################################
# SensorCore wrapper for LTR390
##########################################################################################################
from dataclasses import dataclass

import board

from sensor_core import Sensor, SensorCfg, api
from sensor_core import configuration as root_cfg
from sensor_core.dp_config_objects import Stream
from sensor_core.sensors.drivers import ltr390

logger = root_cfg.setup_logger("sensor_core")

LTR390_STREAM_INDEX = 0
LTR390_SENSOR_INDEX = 83 # LTR390 i2c address, 0x53 (83)
LTR390_SENSOR_TYPE_ID = "LTR390"
LTR390_FIELDS = ["ambient_light", "uv"]

@dataclass
class LTR390SensorCfg(SensorCfg):
    ############################################################
    # SensorCfg fields
    ############################################################
    # The type of sensor.
    sensor_type: api.SENSOR_TYPE = api.SENSOR_TYPE.I2C
    sensor_index: int = LTR390_SENSOR_INDEX
    sensor_model: str = "LTR390"
    # A human-readable description of the sensor model.
    description: str = "LTR390 UV & light sensor"

    ############################################################
    # Custom fields
    ############################################################

DEFAULT_LTR390_SENSOR_CFG = LTR390SensorCfg(
    outputs=[
        Stream(
            description="Ambient light and UV data from LTR390",
            type_id=LTR390_SENSOR_TYPE_ID,
            index=LTR390_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=LTR390_FIELDS,
            cloud_container="sensor-core-journals",
        )
    ],
)

class LTR390(Sensor):
    # Init
    def __init__(self, config: LTR390SensorCfg):
        super().__init__(config)
        self.config = config

    def run(self):
        sensor = None
        while not self.stop_requested.is_set():
            try:
                if sensor is None:
                    i2c = board.I2C()  # uses board.SCL and board.SDA
                    sensor = ltr390.LTR390Driver(i2c)  # type: ignore

                self.log(
                    stream_index=LTR390_STREAM_INDEX,
                    sensor_data={"ambient_light": ("%.1f" % sensor.light),
                                 "uv": ("%.1f" % sensor.uvs)},
                )

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in LTR390 sensor run: {e}", exc_info=True)
            finally:
                logger.debug(f"LTR390 sensor {self.sensor_index} sleeping for "
                             f"{root_cfg.my_device.env_sensor_frequency} seconds")
                self.stop_requested.wait(root_cfg.my_device.env_sensor_frequency)

