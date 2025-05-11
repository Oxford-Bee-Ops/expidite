from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

from sensor_core import api
from sensor_core.utils import utils_clean

############################################################################################
#
# Configuration classes
#
# The system assumes the following files are present in the KEYS directory:
# - keys.env (cloud storage and git keys)
# - sc_config.env (class reference for the fleet config)
#
# The system loads its main config from the fleet_config_py defined in the sc_config.env.
############################################################################################
FAILED_TO_LOAD = "Not set"


@dataclass
class Configuration:
    """Utility super class"""

    def update_field(self, field_name: str, value: Any) -> None:
        setattr(self, field_name, value)

    def update_fields(self, **kwargs: Any) -> None:
        for field_name, value in kwargs.items():
            self.update_field(field_name, value)

    def display(self) -> str:
        display_str = utils_clean.display_dataclass(self)
        return display_str

    def get_field(self, field_name: str) -> Any:
        return getattr(self, field_name)


############################################################################################
# Wifi configuration
############################################################################################
@dataclass
class WifiClient:
    ssid: str
    priority: int
    pw: str

############################################################################################
# Configuration for a device
############################################################################################
@dataclass
class DeviceCfg(Configuration):
    """Configuration for a device"""

    # DPtree objects define the Sensor and DataProcessor objects that will be used to process the data.
    # This field holds a list of function references that when called return the instantiated DPtree objects
    # for this device.
    # We use function references so that we only instantiate the DPtree objects when we need them.
    dp_trees_create_method: Optional[Callable] = None

    name: str = "default"
    device_id: str = "unknown"
    notes: str = "blank"

    # Default cloud container for file upload
    cc_for_upload: str = "sensor-core-upload"

    # Cloud storage container for raw CSV journals uploaded by the device
    cc_for_journals: str = "sensor-core-journals"

    # Cloud storage container for system records (Datasreams: SCORE, SCORP, HEART, WARNING)
    cc_for_system_records: str = "sensor-core-system-records"

    # Cloud container for FAIR records
    cc_for_fair: str = "sensor-core-fair"

    # Frequency of sending device health heart beat
    heart_beat_frequency: int = 60 * 10

    # Default environmental sensor logging frequency in seconds
    env_sensor_frequency: int = 60 * 10

    # Max recording timer in seconds
    # This limits how quickly the system will cleanly shutdown as we wait for all recording 
    # threads to complete. It also limits the duration of any recordings
    max_recording_timer: int = 180

    # Logging: 20=INFO, 10=DEBUG as per logging module
    log_level: int = 20

    # Device management
    attempt_wifi_recovery: bool = True
    manage_leds: bool = True

    # Wifi networks
    # These are the networks that the device will connect to if they are available.
    wifi_clients: list[WifiClient] = field(default_factory=list)


############################################################################################
# Define the two .env files that hold the keys and the SensorCore configuration class ref
############################################################################################
class Keys(BaseSettings):
    """Class to hold the keys for the system"""

    cloud_storage_key: str = FAILED_TO_LOAD
    model_config = SettingsConfigDict(extra="ignore")

    def get_storage_account(self) -> str:
        """Return the storage account name from the key"""
        try:
            # Extract the storage account name from the key
            if "AccountName=" in self.cloud_storage_key:
                storage_account = self.cloud_storage_key.split("AccountName=")[1].split(";")[0]
            else:
                storage_account = self.cloud_storage_key.split("https://")[1].split(".")[0]
            return storage_account
        except Exception as e:
            print(f"Failed to extract storage account from key: {e}")
            return "unknown"


class SystemCfg(BaseSettings):
    """Class to hold the keys for the system"""
    ############################################################
    # Mandatory custom settings
    ############################################################
    # The URL for the Git repo with the user's config and custom sensor code.
    my_git_repo_url: str = FAILED_TO_LOAD
    my_git_branch: str = FAILED_TO_LOAD
    my_git_ssh_private_key_file: str = FAILED_TO_LOAD
    my_fleet_config: str = FAILED_TO_LOAD
    my_start_script: str = FAILED_TO_LOAD
    ############################################################
    # Default-able settings
    ############################################################
    install_type: api.INSTALL_TYPE = api.INSTALL_TYPE.RPI_SENSOR
    # Logging and storage settings
    enable_volatile_logs: str ="Yes"
    # Do you want SensorCore to start automatically after running the rpi_installer.sh script?
    auto_start: str ="Yes"
    # Enable the UFW firewall
    enable_firewall: str = "Yes"
    # Enable use of predictable network interface names
    enable_predictable_interface_names: str = "Yes"
    # Enable the I2C interface on the Raspberry Pi
    enable_i2c: str = "Yes"
    # The location of the virtual environment relative to the $HOME directory.
    # (ie will expand to "$HOME/$venv_dir").
    # This will be created if it does not exist.
    venv_dir: str ="venv"
    # The URL for the Git repo with the SensorCore code.
    # We only need this while SensorCore is in development.
    sensor_core_git_url: str ="github.com:Oxford-Bee-Ops/sensor_core.git"
    sensor_core_git_branch: str ="main"
    # Pydantic-settings helper
    model_config = SettingsConfigDict(extra="ignore")
