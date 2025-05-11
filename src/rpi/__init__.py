# filepath: rpi_core/__init__.py

# Re-export specific classes and functions
# Dynamically fetch the version from the package metadata
import importlib.metadata

from .core import api, configuration
from .core.device_config_objects import DeviceCfg
from .core.dp import DataProcessor
from .core.dp_config_objects import (
    DataProcessorCfg,
    SensorCfg,
    Stream,
)
from .core.dp_tree import DPtree
from .core.sensor import Sensor
from .rpi_core import RpiCore

try:
    __version__ = importlib.metadata.version("expidite")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

# Optionally, define an explicit __all__ to control what gets imported with "from rpi.core import *"
__all__ = [
    "configuration",
    "DataProcessor",
    "DataProcessorCfg",
    "DPtree",
    "DeviceCfg",
    "Sensor",
    "SensorCfg",
    "RpiCore",
    "Stream",
    "api",
]