##########################################################################################################
# RpiCore wrapper for ADXL34X
##########################################################################################################
import time
from dataclasses import dataclass

import pandas as pd

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.sensor import Sensor, SensorCfg
from expidite_rpi.sensors.drivers import adafruit_adxl34x

logger = root_cfg.setup_logger("expidite")

ADXL34X_SENSOR_INDEX = 83 # ADXL34X i2c address, 0x53 (83)
ADXL34X_SENSOR_TYPE_ID = "ADXL34X"
ADXL34X_FIELDS = ["RMS_X", "RMS_Y", "RMS_Z", "RMS_MAGNITUDE",
                  "PEAK_X", "PEAK_Y", "PEAK_Z", "PEAK_MAGNITUDE",
                  "STD_DEV_X", "STD_DEV_Y", "STD_DEV_Z", "STD_DEV_MAGNITUDE",
                  "SAMPLES"]
ADXL34X_STREAM_INDEX = 0
ADXL34X_STREAM: Stream = Stream(
            description="Acceleration data from ADXL34X",
            type_id=ADXL34X_SENSOR_TYPE_ID,
            index=ADXL34X_STREAM_INDEX,
            format=api.FORMAT.LOG,
            fields=ADXL34X_FIELDS,
            cloud_container="expidite-journals",
        )

@dataclass
class ADXL34XSensorCfg(SensorCfg):
    ############################################################
    # Custom fields
    ############################################################
    pass

DEFAULT_ADXL34X_SENSOR_CFG = ADXL34XSensorCfg(
    sensor_type=api.SENSOR_TYPE.I2C,
    sensor_index=ADXL34X_SENSOR_INDEX,
    sensor_model="ADXL34X",
    description="ADXL34X accelerometer",
    outputs=[ADXL34X_STREAM]
)

class ADXL34X(Sensor):
    # Init
    def __init__(self, config: ADXL34XSensorCfg) -> None:
        super().__init__(config)
        self.config = config

    def run(self) -> None:
        accelerometer = None
        while self.continue_recording():
            try:
                if accelerometer is None:
                    accelerometer = adafruit_adxl34x.ADXL343()
                    accelerometer.set_data_rate(adafruit_adxl34x.DataRate.RATE_400_HZ)
                    accelerometer.set_range(adafruit_adxl34x.Range.RANGE_4_G)

                # Capture approximately 1 second of X, Y, and Z instantaneous accelerations readings
                # And stay aligned to 1 real-time second boundaries
                start_time = api.utc_now()
                xyz_df = self.capture_data_block(accelerometer)
                xyz_df['magnitude'] = (xyz_df['aX']**2 + xyz_df['aY']**2 + xyz_df['aZ']**2) ** 0.5

                # Calculate the RMS from the data_df for each of X, Y, Z and the overall vector
                rms = (xyz_df ** 2).mean() ** 0.5

                # Calculate peak acceleration for each of X, Y, Z and overall vector
                peak = xyz_df.max()

                # Calculate the standard deviation for each of X, Y, Z and overall vector
                std_dev = xyz_df.std()

                num_observations = xyz_df.shape[0]

                self.log(
                    stream_index=ADXL34X_STREAM_INDEX,
                    sensor_data={
                        "timestamp": start_time,
                        "RMS_X": ("%.1f" % rms['aX']),
                        "RMS_Y": ("%.1f" % rms['aY']),
                        "RMS_Z": ("%.1f" % rms['aZ']),
                        "RMS_MAGNITUDE": ("%.1f" % rms['magnitude']),
                        "PEAK_X": ("%.1f" % peak['aX']),
                        "PEAK_Y": ("%.1f" % peak['aY']),
                        "PEAK_Z": ("%.1f" % peak['aZ']),
                        "PEAK_MAGNITUDE": ("%.1f" % peak['magnitude']),
                        "STD_DEV_X": ("%.1f" % std_dev['aX']),
                        "STD_DEV_Y": ("%.1f" % std_dev['aY']),
                        "STD_DEV_Z": ("%.1f" % std_dev['aZ']),
                        "STD_DEV_MAGNITUDE": ("%.1f" % std_dev['magnitude']),
                        "SAMPLES": num_observations,
                    },
                )

            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error in ADXL34X sensor run: {e}", exc_info=True)

    def capture_data_block(self, accelerometer: adafruit_adxl34x.ADXL343) -> pd.DataFrame:
        """ Capture approximately 1 second of readings, but stop at a realtime second boundary
        so that captures stay aligned and don't drift over time."""
        data = []
        start_time = int(time.monotonic()) # Round to realtime boundary
        while time.monotonic() - start_time < 1:
            data.append(accelerometer.acceleration)

        # Convert the list of tuples [int, int, int] to a DataFrame
        df = pd.DataFrame(data, columns=["aX", "aY", "aZ"])
        return df
