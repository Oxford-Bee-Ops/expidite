##############################################################################################################
# Expidite API
#
# File define constants used on interfaces between components in the Expidite system.
##############################################################################################################
from datetime import datetime
from enum import Enum, StrEnum
from zoneinfo import ZoneInfo

from azure.storage.blob import StandardBlobTier


##############################################################################################################
# Data record ID fields
##############################################################################################################
class RECORD_ID(StrEnum):
    VERSION = "version_id"
    DATA_TYPE_ID = "data_type_id"
    DEVICE_ID = "device_id"
    SENSOR_INDEX = "sensor_index"
    STREAM_INDEX = "stream_index"
    TIMESTAMP = "timestamp"
    END_TIME = "end_time"
    OFFSET = "primary_offset_index"
    SECONDARY_OFFSET = "secondary_offset_index"
    SUFFIX = "file_suffix"
    INCREMENT = "increment"
    NAME = "device_name"  # Not used programmatically, but helpful for users
    TAGS = "tags"  # Not used programmatically, but helpful for users


REQD_RECORD_ID_FIELDS = [
    RECORD_ID.VERSION.value,
    RECORD_ID.DATA_TYPE_ID.value,
    RECORD_ID.DEVICE_ID.value,
    RECORD_ID.SENSOR_INDEX.value,
    RECORD_ID.STREAM_INDEX.value,
    RECORD_ID.TIMESTAMP.value,
]

ALL_RECORD_ID_FIELDS = [
    *REQD_RECORD_ID_FIELDS,
    RECORD_ID.END_TIME.value,
    RECORD_ID.OFFSET.value,
    RECORD_ID.SECONDARY_OFFSET.value,
    RECORD_ID.SUFFIX.value,
    RECORD_ID.INCREMENT.value,
    RECORD_ID.NAME.value,
    RECORD_ID.TAGS.value,
]


##############################################################################################################
# Sampling override options
##############################################################################################################
class OVERRIDE(StrEnum):
    AUTO = "auto"  # No override
    SAVE = "save"  # Override to save sample
    DISCARD = "discard"  # Override to discard sample


##############################################################################################################
# Installation types
#
# Used in DUA & BCLI
##############################################################################################################
class INSTALL_TYPE(Enum):
    RPI_SENSOR = "rpi_sensor"  # Sensor installation
    SYSTEM_TEST = "system_test"  # System test installation
    ETL = "etl"  # ETL installation
    NOT_SET = "NOT_SET"  # Invalid but used to declare the SensorCfg object


##############################################################################################################
# Blob storage tiers
#
# See Azure documentation for details:
# https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
##############################################################################################################
class StorageTier(Enum):
    """Enum for the supported blob tiers"""

    HOT = StandardBlobTier.HOT
    COOL = StandardBlobTier.COOL
    COLD = StandardBlobTier.ARCHIVE


##############################################################################################################
# Sensor interface type
##############################################################################################################
class SENSOR_TYPE(Enum):
    I2C = "I2C"  # Environmental sensor (e.g., temperature, humidity, etc.)
    USB = "USB"  # Microphone sensor
    CAMERA = "CAMERA"  # Camera sensor
    SYS = "SYS"  # System sensor (e.g., self-tracking)
    NOT_SET = "NOT_SET"  # Invalid but used to declare the SensorCfg object


##############################################################################################################
# Datastream types
##############################################################################################################
class FORMAT(Enum):
    DF = "df"  # Dataframe; can be saved as CSV
    CSV = "csv"  # CSV text format
    LOG = "log"  # JSON-like log format (dict)
    JPG = "jpg"  # JPEG image format
    PNG = "png"  # PNG image format
    MP4 = "mp4"  # MP4 video format
    AVI = "avi"  # AVI video format
    H264 = "h264"  # H264 video format
    WAV = "wav"  # WAV audio format
    TXT = "txt"  # Text format
    YAML = "yaml"  # YAML text format


DATA_FORMATS = [FORMAT.DF, FORMAT.CSV, FORMAT.LOG]


##############################################################################################################
# File naming convention to use on a Stream
#
# File naming conventions are defined in the core.file_naming module.
# A stream can choose to use a specific file naming convention by setting
# this field.
##############################################################################################################
class FILE_NAMING(Enum):
    """Enum for file naming conventions"""

    # Default file naming convention
    DEFAULT = "default"
    # Review mode file naming drops the datetime fields so that each file overwrites the previous one
    REVIEW_MODE = "review_mode"


##############################################################################################################
# Tags used in logs sent from sensors to the ETL
##############################################################################################################
RAISE_WARN_TAG = "RAISE_WARNING#V1"
TELEM_TAG = "TELEM#V1: "

##############################################################################################################
# System Datastream types
##############################################################################################################
HEART_DS_TYPE_ID = "HEART"
WARNING_DS_TYPE_ID = "WARNING"
SCORE_DS_TYPE_ID = "SCORE"
SCORP_DS_TYPE_ID = "SCORP"

SYSTEM_DS_TYPES = [
    HEART_DS_TYPE_ID,
    WARNING_DS_TYPE_ID,
    SCORE_DS_TYPE_ID,
    SCORP_DS_TYPE_ID,
]
SCORP_STREAM_INDEX = 0
SCORE_STREAM_INDEX = 1

##############################################################################################################
# Datetime formats used in the system
#
# All times are in UTC.
#
# The format used for timestamps in the system is "%Y%m%dT%H%M%S%3f"
# (but the %3f directive is not supported by datetime.strptime).
# Nonetheless we only want milliseconds not microseconds in the filenames.
##############################################################################################################
STRFTIME = "%Y%m%dT%H%M%S%f"
PADDED_TIME_LEN = len("20210101T010101000000")


def utc_now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(ZoneInfo("UTC"))


def _to_datetime(t: datetime | float | None) -> datetime:
    if isinstance(t, datetime):
        return t
    if isinstance(t, float):
        return datetime.fromtimestamp(t, tz=ZoneInfo("UTC"))
    return utc_now()


def utc_to_iso_str(t: datetime | float | None = None) -> str:
    """Return the current time in UTC as a formatted string."""
    return _to_datetime(t).isoformat(timespec="milliseconds")


def utc_to_fname_str(t: datetime | float | None = None) -> str:
    """Return the current time in UTC as a string formatted for use in filenames."""
    timestamp = _to_datetime(t).strftime(STRFTIME)
    return timestamp[:-3]


def utc_from_str(t: str) -> datetime:
    """Convert a string timestamp formatted according to a datetime object."""
    # strptime doesn't support just milliseconds, so pad the string with 3 zeros
    t += "0" * (PADDED_TIME_LEN - len(t))

    naive_dt = datetime.strptime(t, STRFTIME)
    # Convert to UTC timezone
    return naive_dt.replace(tzinfo=ZoneInfo("UTC"))


def str_to_iso(t: str) -> str:
    """Convert a string timestamp to an ISO 8601 formatted string."""
    dt = utc_from_str(t)
    return dt.isoformat(timespec="milliseconds")
