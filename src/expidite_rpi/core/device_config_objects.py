from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

from expidite_rpi.utils import utils_clean

##############################################################################################################
#
# Configuration classes
#
# The system assumes the following files are present in the KEYS directory:
# - keys.env (cloud storage and git keys)
# - sc_config.env (class reference for the fleet config)
#
# The system loads its main config from the fleet_config_py defined in the sc_config.env.
##############################################################################################################
FAILED_TO_LOAD = "Not set"


@dataclass
class Configuration:
    """Utility super class"""

    def update_field(self, field_name: str, value: Any) -> None:  # noqa: ANN401
        setattr(self, field_name, value)

    def update_fields(self, **kwargs: Any) -> None:  # noqa: ANN401
        for field_name, value in kwargs.items():
            self.update_field(field_name, value)

    def display(self) -> str:
        return utils_clean.display_dataclass(self)

    def get_field(self, field_name: str) -> Any:  # noqa: ANN401
        return getattr(self, field_name)


##############################################################################################################
# Wifi configuration
##############################################################################################################
@dataclass
class WifiClient:
    ssid: str
    priority: int
    pw: str


##############################################################################################################
# Configuration for a device
##############################################################################################################
@dataclass
class DeviceCfg(Configuration):
    """Configuration for a device"""

    # DPtree objects define the Sensor and DataProcessor objects that will be used to process the data.
    # This field holds a function reference that when called return the instantiated DPtree objects for this
    # device.
    # This method can take optional arguments defined in dp_trees_create_kwargs.
    dp_trees_create_method: Callable | None = None
    dp_trees_create_kwargs: dict | None = None

    name: str = "default"
    device_id: str = "unknown"
    notes: str = "blank"

    # The tags field allows the recording of arbitrary key-value pairs that will be written to the
    # FAIR record. This is useful for recording information about the device in a structured way that can be
    # used in subsequent analysis (eg the location of the deployment).
    tags: dict[str, str] = field(default_factory=dict)

    # The datastore field identifies the blob datastore that this device will use.
    # This string must match the keys_<DATASTORE>.env file name.
    # If the value is "default", then the keys.env file will be used.
    datastore: str = "default"

    # Default cloud container for file upload
    cc_for_upload: str = "expidite-upload"

    # Cloud storage container for raw CSV journals uploaded by the device
    cc_for_journals: str = "expidite-journals"

    # Cloud storage container for system records (Datasreams: SCORE, SCORP, HEART, WARNING)
    cc_for_system_records: str = "expidite-system-records"

    # Cloud container for FAIR records
    cc_for_fair: str = "expidite-fair"
    cc_for_fair_latest: str = "expidite-fair-device-latest"

    # Cloud storage container for system test results
    cc_for_system_test: str = "expidite-system-test"

    # Cloud storage container for diagnostics bundles.
    cc_for_diagnostics_bundles: str = "expidite-diags"

    # Frequency of sending device health heart beat
    heart_beat_frequency: int = 60 * 10

    # Default environmental sensor logging frequency in seconds
    env_sensor_frequency: int = 60 * 10

    # Review mode frequency in seconds
    # This is the frequency at which review mode data is captured.
    review_mode_frequency: int = 5

    # Max recording timer in seconds
    # This limits how quickly the system will cleanly shutdown as we wait for all recording threads to
    # complete. It also limits the duration of any recordings
    max_recording_timer: int = 180

    # Logging: 20=INFO, 10=DEBUG as per logging module
    log_level: int = 20

    # Device management
    attempt_wifi_recovery: bool = True

    # Wifi networks
    # These are the networks that the device will connect to if they are available.
    wifi_clients: list[WifiClient] = field(default_factory=list)

    # List the tests that a system test installation should run.
    # This will be passed to pytest to identify and invoke the tests.
    # This is parameter is passed as the -k option in pytest.
    tests_to_run: list[str] = field(default_factory=list)


##############################################################################################################
# Define the two .env files that hold the keys and the RpiCore configuration class ref
##############################################################################################################
class Keys(BaseSettings):
    """Class to hold the keys for the system"""

    cloud_storage_key: str = FAILED_TO_LOAD
    my_git_pat: str = FAILED_TO_LOAD
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

    ##########################################################################################################
    # Mandatory custom settings
    ##########################################################################################################
    # The URL for the Git repo with the user's config and custom sensor code.
    my_git_repo_url: str = FAILED_TO_LOAD
    # The name of the branch in the Git repo to use.
    my_git_branch: str = "main"
    # The name of the SSH key file in the .expidite directory that gives access to the Git repo if it is
    # private.
    # This can field can be left at FAILED_TO_LOAD if the repo is public.
    my_git_ssh_private_key_file: str = FAILED_TO_LOAD
    # The name of a Python package to install instead of a git clone of the custom sensor code repo.
    # Optional, and most use cases should omit this.
    my_package_name: str = FAILED_TO_LOAD
    # The fully-qualified object name of the fleet config inventory.
    # eg "my_project.my_fleet_config.INVENTORY"
    my_fleet_config: str = FAILED_TO_LOAD
    # The fully-qualified module name to call to start the device.
    # eg "my_project.my_start_script"
    # This is called on reboot or when expidite is started via bcli.
    my_start_script: str = FAILED_TO_LOAD

    ##########################################################################################################
    # Default-able settings
    ##########################################################################################################
    # Logging and storage settings
    enable_volatile_logs: str = "Yes"
    # Do you want RpiCore to start automatically after running the rpi_installer.sh script?
    # Anything other than "Yes" will disable auto-start.
    auto_start: str = "Yes"
    # Enable the UFW firewall
    enable_firewall: str = "Yes"
    # Enable use of predictable network interface names
    enable_predictable_interface_names: str = "Yes"
    # Enable the I2C interface on the Raspberry Pi
    enable_i2c: str = "Yes"
    # The location of the virtual environment relative to the $HOME directory.
    # (ie will expand to "$HOME/$venv_dir").
    # This will be created if it does not exist.
    venv_dir: str = "venv"
    # The branch of expidite code to use.
    expidite_git_branch: str = "main"
    # Pydantic-settings helper
    model_config = SettingsConfigDict(extra="ignore")
    # Manage the LED status indicator
    manage_leds: str = "Yes"

    ##########################################################################################################
    # System test and re-processor settings
    ##########################################################################################################
    # Use local cloud storage for testing if set to "Yes"
    use_local_cloud: str = "No"
    # Local cloud is appended on to the root_working_dir (/expidite)
    local_cloud: str = "local_cloud"
    reprocessor: str = "No"

    ##########################################################################################################
    # For callers to check that the config was loaded OK.
    ##########################################################################################################
    is_valid: bool = False
