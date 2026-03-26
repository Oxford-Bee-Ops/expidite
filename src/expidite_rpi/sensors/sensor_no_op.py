"""A sensor implementation that does nothing."""

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg

logger = root_cfg.setup_logger("expidite")

NO_OP_STREAM: Stream = Stream(
    description="no op",
    type_id="no op",
    index=0,
    format=api.FORMAT.TXT,
    fields=[],
    cloud_container="expidite-journals",
)


DEFAULT_NO_OP_SENSOR_CFG = SensorCfg(
    sensor_type=api.SENSOR_TYPE.I2C,
    sensor_index=0,
    sensor_model="no op",
    description="no op",
    outputs=[NO_OP_STREAM],
)


class NoOp(Sensor):
    def __init__(self, config: SensorCfg) -> None:
        super().__init__(config)
        self.config = config

    def run(self) -> None:

        while self.continue_recording():
            logger.debug(
                f"no-op sensor {self.sensor_index} sleeping for "
                f"{root_cfg.my_device.env_sensor_frequency} seconds"
            )

            if self.in_review_mode():
                wait_period = root_cfg.my_device.review_mode_frequency
            else:
                wait_period = root_cfg.my_device.env_sensor_frequency
            self.stop_requested.wait(wait_period)
