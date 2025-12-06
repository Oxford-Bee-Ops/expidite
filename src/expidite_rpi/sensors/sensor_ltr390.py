##############################################################################################################
# RpiCore wrapper for LTR390
##############################################################################################################
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

logger = root_cfg.setup_logger("expidite")

LTR390_SENSOR_INDEX = 83  # LTR390 i2c address, 0x53 (83)
LTR390_SENSOR_TYPE_ID = "LTR390"
LTR390_FIELDS = ["ambient_light", "uv", "gain"]
LTR390_STREAM_INDEX = 0
LTR390_STREAM: Stream = Stream(
    description="Ambient light and UV data from LTR390",
    type_id=LTR390_SENSOR_TYPE_ID,
    index=LTR390_STREAM_INDEX,
    format=api.FORMAT.LOG,
    fields=LTR390_FIELDS,
    cloud_container="expidite-journals",
)


@dataclass
class LTR390SensorCfg(SensorCfg):
    ##########################################################################################################
    # Custom fields
    ##########################################################################################################
    pass


DEFAULT_LTR390_SENSOR_CFG = LTR390SensorCfg(
    sensor_type=api.SENSOR_TYPE.I2C,
    sensor_index=LTR390_SENSOR_INDEX,
    sensor_model="LTR390",
    description="LTR390 UV & light sensor",
    outputs=[LTR390_STREAM],
)


class LTR390(Sensor):
    # Init
    def __init__(self, config: LTR390SensorCfg) -> None:
        super().__init__(config)
        self.config = config

    def run(self) -> None:
        i2c = board.I2C()
        sensor = adafruit_ltr390.LTR390(i2c)
        current_gain = 4  # This is the index for 18x gain
        sensor.gain = current_gain
        print(sensor.gain)

        while self.continue_recording():
            try:
                light = sensor.light

                # We manage gain to maximise the UV level without saturating the light level
                # Light max value is 65535
                # If light > 60000, decrease the gain to min 0
                # If light < 10000, increase the gain to max 4
                gain_changed = True
                if light > 60000 and current_gain > 0:
                    current_gain -= 1
                    sensor.gain = current_gain
                    logger.debug(f"LTR390 sensor {self.sensor_index} decreasing gain to {current_gain}")
                elif light < 10000 and current_gain < 4:
                    current_gain += 1
                    sensor.gain = current_gain
                    logger.debug(f"LTR390 sensor {self.sensor_index} increasing gain to {current_gain}")
                else:
                    gain_changed = False

                if gain_changed:
                    # Wait a bit for the new gain to take effect
                    self.stop_requested.wait(1)

                self.log(
                    stream_index=LTR390_STREAM_INDEX,
                    sensor_data={
                        "ambient_light": ("%.1f" % sensor.light),
                        "uv": ("%.1f" % sensor.uvs),
                        "gain": sensor.gain,
                    },
                )

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in LTR390 sensor run: {e}", exc_info=True)
            finally:
                if self.in_review_mode():
                    wait_period = root_cfg.my_device.review_mode_frequency
                else:
                    wait_period = root_cfg.my_device.env_sensor_frequency
                logger.debug(f"LTR390 sensor {self.sensor_index} sleeping for {wait_period} seconds")
                self.stop_requested.wait(wait_period)
