##########################################################################################################
# RpiCore wrapper for LTR390
##########################################################################################################
from dataclasses import dataclass

import adafruit_ltr390
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

LTR390_STREAM_INDEX = 0
LTR390_SENSOR_INDEX = 83 # LTR390 i2c address, 0x53 (83)
LTR390_SENSOR_TYPE_ID = "LTR390"
LTR390_FIELDS = ["ambient_light", "uv", "gain"]

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
            cloud_container="expidite-journals",
        )
    ],
)

class LTR390(Sensor):
    # Init
    def __init__(self, config: LTR390SensorCfg):
        super().__init__(config)
        self.config = config

    def run(self):

        i2c = board.I2C()
        sensor = adafruit_ltr390.LTR390(i2c)
        sensor.gain = 3 # This is the index for 9x gain
        print(sensor.gain)

        while self.continue_recording():
            try:
                self.log(
                    stream_index=LTR390_STREAM_INDEX,
                    sensor_data={"ambient_light": ("%.1f" % sensor.light),
                                 "uv": ("%.1f" % sensor.uvs),
                                 "gain": sensor.gain},
                )

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in LTR390 sensor run: {e}", exc_info=True)
            finally:
                logger.debug(f"LTR390 sensor {self.sensor_index} sleeping for "
                             f"{root_cfg.my_device.env_sensor_frequency} seconds")
                self.stop_requested.wait(root_cfg.my_device.env_sensor_frequency)

