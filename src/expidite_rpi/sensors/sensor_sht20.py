from dataclasses import dataclass

from sensirion_i2c_driver import I2cConnection, LinuxI2cTransceiver
from sensirion_i2c_sht.sht2x.device import Sht2xI2cDevice
from sensirion_i2c_sht.sht2x.response_types import Sht2xHumidity, Sht2xTemperature

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg

logger = root_cfg.setup_logger("expidite")

SHT20_STARTUP_RETRIES = 3
SHT20_STARTUP_RETRY_DELAY_SECONDS = 0.2

SHT20_SENSOR_INDEX = 64  # SHT20 i2c address, 0x40(64)
SHT20_SENSOR_TYPE_ID = "SHT20"
SHT20_FIELDS = ["temperature", "humidity"]
SHT20_STREAM_INDEX = 0
SHT20_STREAM: Stream = Stream(
    description="Temperature and humidity data from SHT20",
    type_id=SHT20_SENSOR_TYPE_ID,
    index=SHT20_STREAM_INDEX,
    format=api.FORMAT.LOG,
    fields=SHT20_FIELDS,
    cloud_container="expidite-journals",
)


@dataclass
class SHT20SensorCfg(SensorCfg):
    ##########################################################################################################
    # Custom fields
    ##########################################################################################################
    pass


DEFAULT_SHT20_SENSOR_CFG = SHT20SensorCfg(
    sensor_type=api.SENSOR_TYPE.I2C,
    sensor_index=SHT20_SENSOR_INDEX,
    sensor_model="SHT20",
    description="SHT20 Temperature and Humidity sensor",
    outputs=[SHT20_STREAM],
)


class SHT20(Sensor):
    # Init
    def __init__(self, config: SHT20SensorCfg) -> None:
        super().__init__(config)
        self.config = config

    # Separate thread to log data
    def run(self) -> None:
        while self.continue_recording():
            try:
                with LinuxI2cTransceiver("/dev/i2c-1") as i2c_transceiver:
                    connection = I2cConnection(i2c_transceiver)
                    sensor = Sht2xI2cDevice(connection, slave_address=0x40)
                    sensor.soft_reset()

                    while self.continue_recording():
                        try:
                            response = sensor.single_shot_measurement()
                            if not isinstance(response, tuple) or len(response) != 2:
                                msg = (
                                    f"{root_cfg.RAISE_WARN()}Unexpected response format "
                                    f"from SHT20 sensor: {response}"
                                )
                                logger.error(msg)
                                continue

                            temperature: Sht2xTemperature = response[0]
                            humidity: Sht2xHumidity = response[1]

                            if temperature is None or humidity is None:
                                logger.error(f"{root_cfg.RAISE_WARN()}Error in SHT20 sensor run: No data")
                                continue

                            temperature_c = temperature.degrees_celsius
                            humidity_rh = humidity.percent_rh

                            self.log(
                                stream_index=SHT20_STREAM_INDEX,
                                sensor_data={
                                    "temperature": (f"{temperature_c:.1f}"),
                                    "humidity": (f"{humidity_rh:.1f}"),
                                },
                            )
                            logger.debug(
                                f"SHT20 sensor {self.sensor_index} data: "
                                f"{temperature_c:.1f}C, {humidity_rh:.1f}%"
                            )

                        except OSError:
                            logger.warning(
                                f"{root_cfg.RAISE_WARN()}SHT20 I2C transient failure for sensor "
                                f"{self.sensor_index}; reinitializing bus"
                            )
                            break
                        except Exception:
                            logger.exception(f"{root_cfg.RAISE_WARN()}Error in SHT20 sensor run")
                        finally:
                            if self.in_review_mode():
                                wait_period = root_cfg.my_device.review_mode_frequency
                            else:
                                wait_period = root_cfg.my_device.env_sensor_frequency
                            logger.debug(
                                f"SHT20 sensor {self.sensor_index} sleeping for {wait_period} seconds"
                            )
                            self.stop_requested.wait(wait_period)
            except Exception:
                logger.warning(f"{root_cfg.RAISE_WARN()}SHT20 startup failed; will retry")
                self.stop_requested.wait(SHT20_STARTUP_RETRY_DELAY_SECONDS)
