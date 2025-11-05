from dataclasses import dataclass
from time import sleep

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from sensirion_driver_adapters.i2c_adapter.i2c_channel import I2cChannel
from sensirion_i2c_driver import CrcCalculator, I2cConnection, LinuxI2cTransceiver
from sensirion_i2c_sht4x.device import Sht4xDevice

logger = root_cfg.setup_logger("expidite")

SHT40_STREAM_INDEX = 0
SHT40_SENSOR_INDEX = 68 # SHT40 i2c address, 0x44(68)
SHT40_SENSOR_TYPE_ID = "SHT40"
SHT40_FIELDS = ["temperature", "humidity"]

@dataclass
class SHT40SensorCfg(SensorCfg):
    ############################################################
    # SensorCfg fields
    ############################################################
    sensor_type: api.SENSOR_TYPE = api.SENSOR_TYPE.I2C
    sensor_index: int = SHT40_SENSOR_INDEX
    sensor_model: str = "SHT40"
    description: str = "SHT40 Temperature and Humidity sensor"

DEFAULT_SHT40_SENSOR_CFG = SHT40SensorCfg(
    outputs=[
        Stream(
            description="Temperature and humidity data from SHT40",
            type_id=SHT40_SENSOR_TYPE_ID,
            index=SHT40_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=SHT40_FIELDS,
            cloud_container="expidite-journals",
        )
    ],
)


class SHT40(Sensor):
    # Init
    def __init__(self, config: SHT40SensorCfg):
        super().__init__(config)
        self.config = config
        
    # Separate thread to log data
    def run(self):

        with LinuxI2cTransceiver("/dev/i2c-1") as i2c_transceiver:
            channel = I2cChannel(I2cConnection(i2c_transceiver),
                                slave_address=0x44,
                                crc=CrcCalculator(8, 0x31, 0xff, 0x0))
            sensor = Sht4xDevice(channel)

            try:
                sensor.soft_reset()
                sleep(0.01)
            except Exception:
                logger.warning(f"{root_cfg.RAISE_WARN()}Could not reset SHT40 sensor {self.sensor_index}")
            serial_number = sensor.serial_number()
            logger.debug(f"SHT40 serial_number: {serial_number}; ")

            while self.continue_recording():
                try:
                    temperature, humidity = sensor.measure_medium_precision()

                    if temperature is None or humidity is None:
                        logger.error(f"{root_cfg.RAISE_WARN()}Error in SHT40 sensor run: No data")
                        continue

                    self.log(
                        stream_index=SHT40_STREAM_INDEX,
                        sensor_data={"temperature": ("%.1f" % temperature.value),
                                     "humidity": ("%.1f" % humidity.value)},
                    )
                    logger.debug(f"SHT40 sensor {self.sensor_index} data: "
                                 f"{temperature.value:.1f}C, {humidity.value:.1f}%")

                    # If the humidity is high, run a heating cycle to prevent condensation
                    # We do it after measurement to avoid affecting the reading
                    # It takes a few seconds to heat up and cool down
                    if humidity.value > 90:
                        logger.info(f"SHT40 sensor {self.sensor_index} high humidity "
                                    f"{humidity.value:.1f}%, activating heater")
                        sensor.activate_medium_heater_power_long()

                except Exception as e:
                    logger.error(f"{root_cfg.RAISE_WARN()}Error in SHT40 sensor run: {e}", exc_info=True)
                finally:
                    if self.in_review_mode():
                        wait_period = root_cfg.my_device.review_mode_frequency
                    else:
                        wait_period = root_cfg.my_device.env_sensor_frequency
                    logger.debug(f"SHT40 sensor {self.sensor_index} sleeping for "
                                f"{wait_period} seconds")
                    self.stop_requested.wait(wait_period)

