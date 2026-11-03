# filepath: rpi_core/__init__.py

# Re-export specific classes and functions
# Dynamically fetch the version from the package metadata
import importlib.metadata
import os

# Blinka 10+ prompts to install missing platform packages on `import board` in interactive terminals,
# including optional PWM and NeoPixel backends that we do not use. Disable this automatic check and
# installation prompt before any `import board`. We do require gpiod, but that is automatically installed as
# part of the OS.
os.environ.setdefault("BLINKA_SKIP_DEPENDENCY_CHECK", "1")

from .core import api, configuration, file_naming
from .core.device_config_objects import DeviceCfg, WifiClient
from .core.dp import DataProcessor
from .core.dp_config_objects import (
    DataProcessorCfg,
    SensorCfg,
    Stream,
)
from .core.dp_tree import DPtree
from .core.sensor import Sensor
from .rpi_core import RpiCore
from .utils import rpi_emulator

try:
    __version__: str = importlib.metadata.version("expidite")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

# Optionally, define an explicit __all__ to control what gets imported with "from rpi.core import *"
__all__ = [
    "DPtree",
    "DataProcessor",
    "DataProcessorCfg",
    "DeviceCfg",
    "RpiCore",
    "Sensor",
    "SensorCfg",
    "Stream",
    "WifiClient",
    "api",
    "configuration",
    "file_naming",
    "rpi_emulator",
]
