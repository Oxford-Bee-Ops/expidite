from expidite_rpi.core.device_config_objects import DeviceCfg, WifiClient
from expidite_rpi.core.dp_tree import DPtree
from expidite_rpi.example.my_processor_example import EXAMPLE_FILE_PROCESSOR_CFG, ExampleProcessor
from expidite_rpi.example.my_sensor_example import (
    EXAMPLE_FILE_STREAM_INDEX,
    EXAMPLE_SENSOR_CFG,
    ExampleSensor,
)

###############################################################################
# RpiCore config model
#
# At the top level, we are defining configuration for a fleet of devices.
# This fleet config must be returned as a list of DeviceCfg objects.
# The inventory is passed to RpiCore when it is first configured:
#
#   RpiCore.configure(fleet_config=example.my_fleet_config.INVENTORY)
#
# The DeviceCfg contains:
# - name: a friendly name for the device (eg Alex)
# - notes: free-form notes on what the device is being used for (eg "Experiment 1")
# - system: a SystemCfg object which defines how the system parameters, such as cloud storage configuration
# - sensor_ds_list: a list of SensorDsCfg objects defining each Sensor and its associated Datastreams
#
# A Datastream defines a source of data coming from a Sensor.
# A Sensor may produce multiple Datastreams, each with a different type of data.
# The Sensor configuration is stored in a SensorCfg object.
# The Datastream configuration is stored in a DatastreamCfg object.
# The combined config of a sensor and its datastreams are in a SensorDsCfg object.
#
# The data produced by a Datastream (eg video files) may be processed by 0 or more DataProcessors.
# In the video file example, a DataProcessor might use an ML algorithm to identify bees in a video
# and output the number of bees identified.
# DataProcessors act in a chain, with data being passed from one to the next.
# The DataProcessors associated with a Datastream are defined on the DatastreamCfg
# as lists of DataProcessorCfg objects.
# There are two lists:
#  - EdgeProcessors that act on the device
#  - CloudProcessors that act as part of a subsequent ETL on a server or in the cloud.
#
# DeviceCfg (1 per physical device)
# -> sensor_ds_list: list[SensorDsCfg] - 1 per Sensor)
#    -> [0]
#       -> sensor_cfg: SensorCfg
#       -> datastream_cfgs: list[DatastreamCfg]
#          -> [0]
#             -> edge_processors: list[DataProcessorCfg]
#             -> cloud_processors: list[DataProcessorCfg]
#
###############################################################################

# Pre-configure the devices with awareness of wifi APs
WIFI_CLIENTS: list[WifiClient] = [
    WifiClient("bee-ops", 100, "abcdabcd"),
    WifiClient("bee-ops-zone", 85, "abcdabcd"),
    WifiClient("bee-ops-zone1", 80, "abcdabcd"),
    WifiClient("bee-ops-zone2", 70, "abcdabcd"),
]


def create_example_device() -> list[DPtree]:
    """Create a standard camera device."""
    my_sensor = ExampleSensor(EXAMPLE_SENSOR_CFG)
    my_dp = ExampleProcessor(EXAMPLE_FILE_PROCESSOR_CFG, my_sensor.sensor_index)
    my_tree = DPtree(my_sensor)
    my_tree.connect((my_sensor, EXAMPLE_FILE_STREAM_INDEX), my_dp)
    return [my_tree]


###############################################################################
# Define per-device configuration for the fleet of devices
###############################################################################
INVENTORY: list[DeviceCfg] = [
    DeviceCfg(  # This is the DUMMY MAC address for windows
        name="DUMMY",
        device_id="d01111111111",
        notes="Using Dummy as an all-defaults camera in Experiment A",
        dp_trees_create_method=create_example_device,
        wifi_clients=WIFI_CLIENTS,
        tags={
            "Row": "132",
            "Column": "2",
            "Location": "Gantry",
        },
    ),
]
