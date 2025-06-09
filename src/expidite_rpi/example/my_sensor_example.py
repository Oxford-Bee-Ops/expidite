
from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import SensorCfg, Stream
from expidite_rpi.core.sensor import Sensor

logger = root_cfg.setup_logger("expidite")

EXAMPLE_LOG_TYPE_ID = "DUMML"
EXAMPLE_FILE_TYPE_ID = "DUMMF"

EXAMPLE_FILE_STREAM_INDEX = 0
EXAMPLE_LOG_STREAM_INDEX = 1

#############################################################################################################
# Define the SensorCfg object for the ExampleSensor
#
# We've added a_custom_field to demonstrate passing custom configuration to a concrete subclass of Sensor.
#############################################################################################################
EXAMPLE_SENSOR_CFG = SensorCfg(
    # The type of sensor.
    sensor_type = api.SENSOR_TYPE.I2C,
    # Sensor index
    sensor_index = 1,
    sensor_model="ExampleSensor",
    # A human-readable description of the sensor model.
    description = "Dummy sensor for testing purposes",
    # The list of data output streams from the sensor.
    outputs=[
        Stream("Example image file stream",
                EXAMPLE_FILE_TYPE_ID, 
                EXAMPLE_FILE_STREAM_INDEX, 
                api.FORMAT.JPG, 
                ["temperature"],
                cloud_container="expidite-upload",
                sample_probability="1.0"),
        Stream("Example log file stream",
                EXAMPLE_LOG_TYPE_ID, 
                EXAMPLE_LOG_STREAM_INDEX, 
                api.FORMAT.LOG, 
                ["temperature"]),
    ],
)

#############################################################################################################
# Define the ExampleSensor as a concrete implementation of the Sensor class
#
# A concrete Sensor class must implement the run() method.
#############################################################################################################
class ExampleSensor(Sensor):
    def __init__(self, config: SensorCfg) -> None:
        super().__init__(config)
        self.value_ticker = 0

    def run(self) -> None:
        """The run method is called when the Sensor is started."""

        # Main sensor loop
        # All sensor implementations must check for stop_requested to allow the sensor to be stopped cleanly
        while self.continue_recording():
            self.value_ticker += 1
            self.log(stream_index=EXAMPLE_LOG_STREAM_INDEX,
                     sensor_data={"temperature": self.value_ticker},)
            
            # We intentionally kill the thread after 25 iterations to allow the DP processing to complete.
            if self.value_ticker > 25:
                break

            fname = file_naming.get_temporary_filename(api.FORMAT.JPG)
            # Generate a random image file
            with open(fname, "w") as f:
                f.write("This is a dummy image file")
            self.save_recording(stream_index=EXAMPLE_FILE_STREAM_INDEX, 
                                temporary_file=fname, 
                                start_time=api.utc_now())

            # Sensors should not sleep for more than ~180s so that the stop_requested flag can be checked
            # and the sensor shut down cleanly in a reasonable time frame.
            if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
                # In test mode, sleep for 0.1s to allow the test to run quickly
                self.stop_requested.wait(0.1)
            else:
                self.stop_requested.wait(10)
