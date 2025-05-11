from dataclasses import dataclass
from typing import Optional

from sensor_core import api, file_naming
from sensor_core import configuration as root_cfg

logger = root_cfg.setup_logger("sensor_core")

@dataclass
class Stream:
    """Defines the format and fields present in a datastream coming from a DPtreeNode."""
    # Human-understandable description of the data in the stream
    description: str
    # Used to identify the type & purpose of data in file names, etc.
    # In combination with the index, this will be unique to a given sensor.
    # In combination with the device_id & sensor_index this must be globally unique.
    type_id: str
    # Idenfier for the output stream.
    index: int
    # The type of data being produced by this output stream.
    format: api.FORMAT
    # The human-readable name of the output stream.
    fields: Optional[list[str]] = None
    # The cloud storage container to which the data is archived.
    # This is required for all types uploading files, other than output.format="DF".
    # "DF" data is uploaded to the DeviceCfg.cc_for_journals container.
    cloud_container: Optional[str] = None

    # Some sources support saving of sample raw recordings to the archive.
    # This string is interpreted by the Sensor or DataProcessor to determine the frequency of 
    # raw data sampling. The format of this string is specific to the Sensor or DataProcessor.
    # The default implementation interprets this string as a float sampling probability (0.0-1.0)
    sample_probability: Optional[str] = None

    # Storage tier for the data in the cloud storage system.
    # If you're using Azure for storage, see Azure documentation for details:
    # https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
    storage_tier: api.StorageTier = api.StorageTier.COOL


    def get_data_id(self, sensor_index: int) -> str:
        """
        Returns the unique identifier for this node.  Used in filenaming and other data management.

        Returns:
            The unique identifier for this node.
        """
        return file_naming.create_data_id(root_cfg.my_device_id, sensor_index, self.type_id, self.index)

@dataclass
class DPtreeNodeCfg:
    """Defines the configuration for a node in the DPtree.
    SensorCfg & DataProcessorCfg inherit from this class.
    """
    outputs: list[Stream]
    
    # Human-meaningful description of the node.
    description: str


@dataclass
class SensorCfg(DPtreeNodeCfg):
    """Defines the configuration for a concrete Sensor class implementation.
    Can be subclassed to add additional configuration parameters specific to the Sensor class.

    Parameters:
    ----------
    sensor_type: str
        One of the sensor types defined in api.SensorType.
        These represent physical interface types on the Raspberry Pi (eg I2C, USB, Camera).

    sensor_index: int
        The index of the sensor - represents the phsyical interface index or comms port.
        For example, the physical USB port index or the I2C signaling channel.
        Must be unique in combination with the sensor_type.

    sensor_model: str
        The device model of the sensor (eg AHT20, PiCameraModule2).
    """
    sensor_type: api.SENSOR_TYPE = api.SENSOR_TYPE.NOT_SET
    sensor_index: int = 0
    sensor_model: str = root_cfg.FAILED_TO_LOAD


@dataclass
class DataProcessorCfg(DPtreeNodeCfg):
    """Defines the configuration for a concrete DataProcessor class implementation.
    Can be subclassed to add additional configuration parameters specific to the DataProcessor class."""
    