###################################################################################################
# This file contains recipes for fully specified device types.
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
from dataclasses import replace

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_tree import DPtree
from expidite_rpi.sensors import processor_video_aruco
from expidite_rpi.sensors.processor_video_trapcam import (
    DEFAULT_TRAPCAM_DP_CFG,
    TrapcamDp,
)
from expidite_rpi.sensors.sensor_adxl34x import ADXL34X, DEFAULT_ADXL34X_SENSOR_CFG
from expidite_rpi.sensors.sensor_aht20 import AHT20, DEFAULT_AHT20_SENSOR_CFG
from expidite_rpi.sensors.sensor_audio_on_demand import DEFAULT_AUDIO_SENSOR_CFG, AudioSensor
from expidite_rpi.sensors.sensor_bmp280 import BMP280, DEFAULT_BMP280_SENSOR_CFG
from expidite_rpi.sensors.sensor_ltr390 import DEFAULT_LTR390_SENSOR_CFG, LTR390
from expidite_rpi.sensors.sensor_rpicam_vid import (
    DEFAULT_RPICAM_SENSOR_CFG,
    RPICAM_STREAM_INDEX,
    RpicamSensor,
    RpicamSensorCfg,
)
from expidite_rpi.sensors.sensor_sht31 import DEFAULT_SHT31_SENSOR_CFG, SHT31
from expidite_rpi.sensors.sensor_sht40 import DEFAULT_SHT40_SENSOR_CFG, SHT40
from expidite_rpi.sensors.sensor_video_on_demand import DEFAULT_VIDEO_OD_SENSOR_CFG, VideoOnDemandSensor

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
# Create SHT40 temp and humidity sensor device
######################################################################################################
def create_sht40_device() -> list[DPtree]:
    cfg = DEFAULT_SHT40_SENSOR_CFG
    my_sensor = SHT40(cfg)
    my_tree = DPtree(my_sensor)
    return [my_tree]


######################################################################################################
# Create BMP280 pressure sensor device
######################################################################################################
def create_bmp280_device() -> list[DPtree]:
    cfg = DEFAULT_BMP280_SENSOR_CFG
    my_sensor = BMP280(cfg)
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


######################################################################################################
# Create ADXL34x acceleration sensor device
######################################################################################################
def create_adxl34x_device() -> list[DPtree]:
    cfg = DEFAULT_ADXL34X_SENSOR_CFG
    my_sensor = ADXL34X(cfg)
    my_tree = DPtree(my_sensor)
    return [my_tree]


###################################################################################################
# Create LTR390 light and UV sensor device
###################################################################################################
def create_ltr390_device() -> list[DPtree]:
    cfg = DEFAULT_LTR390_SENSOR_CFG
    my_sensor = LTR390(cfg)
    my_tree = DPtree(my_sensor)
    return [my_tree]


###################################################################################################
# Low FPS continuous video recording device
###################################################################################################
def create_continuous_video_4fps_device() -> list[DPtree]:
    """Create a standard camera device.
    No recordings are saved to the cloud - it is assumed a DP will process the recordings locally."""
    sensor_index = 0
    cfg: RpicamSensorCfg = replace(
        DEFAULT_RPICAM_SENSOR_CFG,
        description="Low FPS continuous video recording device",
        sensor_index=sensor_index,
        rpicam_cmd="rpicam-vid --framerate 4 --width 640 --height 480 -o FILENAME -t 180000 -v 0",
    )
    my_sensor = RpicamSensor(cfg)
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
def create_trapcam_device(sensor_index: int | None = 0) -> list[DPtree]:
    """Create a standard camera device."""
    if sensor_index is None:
        sensor_index = 0

    # Define the sensor
    # Use the default rpicam sensor config except for the rpicam command
    cfg: RpicamSensorCfg = replace(
        DEFAULT_RPICAM_SENSOR_CFG,
        rpicam_cmd="rpicam-vid --framerate 15 --width 640 --height 480 -o FILENAME -t 180000",
    )
    my_sensor = RpicamSensor(cfg)

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
        processor_video_aruco.DEFAULT_AUROCO_PROCESSOR_CFG, sensor_index=sensor_index
    )

    # Connect the DataProcessor to the Sensor
    my_tree = DPtree(my_sensor)
    my_tree.connect(
        source=(my_sensor, RPICAM_STREAM_INDEX),
        sink=my_dp,
    )
    return [my_tree]


#####################################################################################################
# On-demand audio and video devices
#####################################################################################################
def create_on_demand_audio_video_device() -> list[DPtree]:
    """Create a device that has both on-demand audio and video sensors.
    Recording is triggered via the BCLI sensing options."""
    # Audio Sensor
    audio_cfg = DEFAULT_AUDIO_SENSOR_CFG
    audio_cfg.sensor_index = 1
    my_audio_sensor = AudioSensor(audio_cfg)

    # Video Sensor
    video_cfg = DEFAULT_VIDEO_OD_SENSOR_CFG
    video_cfg.sensor_index = 0
    my_video_sensor = VideoOnDemandSensor(video_cfg)

    # Create DPtrees for each sensor
    my_audio_tree = DPtree(my_audio_sensor)
    my_video_tree = DPtree(my_video_sensor)

    # Return both trees as a list
    return [my_audio_tree, my_video_tree]
