###################################################################################################
# Thie file contains recipes for fully specified device types.
#
# RpiCore config model
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
###################################################################################################
from typing import Optional

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.core.dp_tree import DPtree
from expidite_rpi.sensors import processor_video_aruco
from expidite_rpi.sensors.processor_video_trapcam import (
    DEFAULT_TRAPCAM_DP_CFG,
    TrapcamDp,
)
from expidite_rpi.sensors.sensor_aht20 import AHT20, DEFAULT_AHT20_SENSOR_CFG
from expidite_rpi.sensors.sensor_rpicam_vid import (
    DEFAULT_RPICAM_SENSOR_CFG,
    RPICAM_DATA_TYPE_ID,
    RPICAM_STREAM_INDEX,
    RpicamSensor,
    RpicamSensorCfg,
)
from expidite_rpi.sensors.sensor_sht31 import DEFAULT_SHT31_SENSOR_CFG, SHT31

logger = root_cfg.setup_logger("expidite")


######################################################################################################
# Create SHT31 temp and humidity sensor device
######################################################################################################
def create_sht31_device() -> list[DPtree]:
    cfg = DEFAULT_SHT31_SENSOR_CFG
    my_sensor = SHT31(cfg)
    my_tree = DPtree(my_sensor)
    return [my_tree]
    

######################################################################################################
# Create AHT20 temp and humidity sensor device
######################################################################################################
def create_aht20_device() -> list[DPtree]:
    cfg = DEFAULT_AHT20_SENSOR_CFG
    my_sensor = AHT20(cfg)
    my_tree = DPtree(my_sensor)
    return [my_tree]


###################################################################################################
# Low FPS continuous video reording device
###################################################################################################
def create_continuous_video_4fps_device() -> list[DPtree]:
    """Create a standard camera device.
    No recordings are saved to the cloud - it is assumed a DP will process the recordings locally."""
    sensor_index = 0
    sensor_cfg = RpicamSensorCfg(
        description="Low FPS continuous video recording device",
        sensor_index=sensor_index,
        outputs=[
            Stream(
                description="Low FPS continuous video recording",
                type_id=RPICAM_DATA_TYPE_ID,
                index=RPICAM_STREAM_INDEX,
                format=api.FORMAT.MP4,
                cloud_container="expidite-upload",
                sample_probability="0.0",
            )
        ],
        rpicam_cmd="rpicam-vid --framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0"
    )
    my_sensor = RpicamSensor(sensor_cfg)
    my_tree = DPtree(my_sensor)
    return [my_tree]


###################################################################################################
# Trap cameras
#
# We start with a low FPS continuous video recording device, and add a trap camera processor to it.
# This creates a derived TRAP_CAM_DS datastream with the sub-sampled videos.
# The original continuous video recording is deleted after being passed to the trap cam DP; 
# we could opt to save raw samples if we wanted to.
###################################################################################################
def create_trapcam_device(sensor_index: Optional[int] = 0) -> list[DPtree]:
    """Create a standard camera device."""

    if sensor_index is None:
        sensor_index = 0
        
    # Define the sensor
    my_sensor = RpicamSensor(
        RpicamSensorCfg(
            sensor_type=api.SENSOR_TYPE.CAMERA,
            sensor_index=sensor_index,
            sensor_model="PiCameraModule3",
            description="Video sensor that uses rpicam-vid",
            outputs=[
                Stream(
                    description="Basic continuous video recording.",
                    type_id=RPICAM_DATA_TYPE_ID,
                    index=RPICAM_STREAM_INDEX,
                    format=api.FORMAT.MP4,
                    cloud_container="expidite-upload",
                    sample_probability="0.0",
                )
            ],
            rpicam_cmd = "rpicam-vid --framerate 15 --width 640 --height 480 -o FILENAME -t 180000",
        )
    )

    # Define the DataProcessor
    my_dp = TrapcamDp(DEFAULT_TRAPCAM_DP_CFG, sensor_index=sensor_index)

    # Connect the DataProcessor to the Sensor
    my_tree = DPtree(my_sensor)
    my_tree.connect(
        source=(my_sensor, RPICAM_STREAM_INDEX),
        sink=my_dp,
    )
    return [my_tree]

def create_double_trapcam_device() -> list[DPtree]:
    camera1 = create_trapcam_device(sensor_index=0)
    camera2 = create_trapcam_device(sensor_index=1)
    return camera1 + camera2


####################################################################################################
# Aruco camera device
####################################################################################################
def create_aruco_camera_device(sensor_index: int) -> list[DPtree]:
    """Create a device that spots aruco markers."""

    # Sensor
    cfg = DEFAULT_RPICAM_SENSOR_CFG
    cfg.sensor_index = sensor_index
    my_sensor = RpicamSensor(cfg)

    # DataProcessor
    my_dp = processor_video_aruco.VideoArucoProcessor(
        processor_video_aruco.DEFAULT_AUROCO_PROCESSOR_CFG,
        sensor_index=sensor_index)

    # Connect the DataProcessor to the Sensor
    my_tree = DPtree(my_sensor)
    my_tree.connect(
        source=(my_sensor, RPICAM_STREAM_INDEX),
        sink=my_dp,
    )
    return [my_tree]

