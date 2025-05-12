from typing import Optional

from expidite_rpi.core import config_validator
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.device_config_objects import DeviceCfg
from expidite_rpi.core.device_health import DeviceHealth
from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator

logger = root_cfg.setup_logger("rpi_core")

####################################################################################################
# RpiCore provides the public interface to the rpi_core module.
# It is the entry point for users to configure and start the rpi.
# Since the RpiCore may already be running (for example from boot in crontab), we can't assume
# that this is the only instance of RpiCore on this device.
# Therefore, all actions need to be taken indirectly via file flags or system calls.
####################################################################################################


class RpiCore:
    """
    RpiCore provides the public interface to the rpi_core module.
    """
    # We make the location of the keys file a public variable so that users can reference
    # it in their own code.
    KEYS_FILE = root_cfg.KEYS_FILE


    def load_configuration(self) -> list[DeviceCfg] | None:
        """ Load the configuration specified in the system.cfg file found in $HOME/.rpi.
        The output can be passed to test_configuration() or configure().
        """
        return root_cfg.load_configuration()


    def test_configuration(self, 
                           fleet_config: list[DeviceCfg], 
                           device_id: Optional[str] = None) -> tuple[bool, list[str]]:
        """ Validates that the configuration in fleet_config is valid.

        Parameters:
        - fleet_config: The configuration to be validated.
        - device_id: The device config to validate. If None, all config is validated.

        Returns:
        - A tuple containing a boolean indicating if the configuration is valid and a list of error messages.
        - If the configuration is valid, the list of error messages will be empty.
        """
        is_valid = True
        errors: list[str] = []

        if not fleet_config:
            return (False, ["No configuration provided."])
        
        try:
            for device in fleet_config:
                if device_id is not None and device.device_id != device_id:
                    logger.debug(f"Skipping device {device.device_id} for validation.")
                    continue
                # Check the device configuration is valid
                logger.debug(f"Validating device {device.device_id} configuration.")
                dp_trees = EdgeOrchestrator._safe_call_create_method(device.dp_trees_create_method)
                is_valid, errors = config_validator.validate_trees(dp_trees)
                if not is_valid:
                    errors.append(f"Invalid configuration for device {device.device_id}: {errors}")
                    break
        except Exception as e:
            errors.append(str(e))

        return (is_valid, errors)                


    def configure(self, fleet_config: list[DeviceCfg]) -> None:
        """
        Set the RpiCore configuration.
        See the /examples folder for configuration file templates.

        Parameters:
        - fleet_config_py: Inventory class that implements get_invemtory()
        - force_update: If True, the configuration will be reloaded and the device rebooted
            even if RpiCore is already running.

        Raises:
        - Exception: If the RpiCore is running (and force_update is not set).
        - Exception: If no configuration exists.
        """
        if not fleet_config:
            raise Exception("No configuration files provided.")
        
        success, error = root_cfg.check_keys()
        if not success:
            raise Exception(error)

        # Find the config for this device
        logger.info(f"TEST_CREATE of fleet config with {len(fleet_config)} devices.")
        is_valid, errors = self.test_configuration(fleet_config, root_cfg.my_device_id)
        if not is_valid:
            raise ValueError(f"Configuration is not valid: {errors}")
        logger.info("Completed TEST_CREATE of fleet config.")

        # Load the configuration
        root_cfg.set_inventory(fleet_config)

    def start(self) -> None:
        """
        Start the rpi_core to begin data collection.

        Raises:
        - Exception: If the RpiCore is not configured.
        """
        if not self._is_configured() or root_cfg.system_cfg is None:
            raise Exception("RpiCore must be configured before starting.")

        logger.info("Starting RpiCore")

        # Start the orchestrator, which will start the sensors
        # This will run the sensors in the current process, so it will exit when the process exits.
        EdgeOrchestrator.start_all_with_watchdog()


    def stop(self) -> None:
        """
        Stop RpiCore.
        And remove any crontab entries added by make_my_script_persistent.
        """
        # Ask the EdgeOrchestrator to stop all sensors
        print(f"RpiCore stopping - this may take up to {root_cfg.my_device.max_recording_timer}s.")
        EdgeOrchestrator.get_instance().stop_all()

    def status(self, verbose: bool = True) -> str:
        """
        Get the current status of the RpiCore.

        Return:
        - A string describing the status of the RpiCore.
        """
        display_message = "\n"

        # Check config is clean
        success, error = root_cfg.check_keys()
        if not success:
            display_message += f"\n\n{error}"

        # Display the orchestrator status
        orchestrator = EdgeOrchestrator.get_instance()
        if orchestrator is not None:
            display_message += (f"\n\nRpiCore running: {orchestrator.watchdog_file_alive()}\n")

            if verbose:
                status = orchestrator.status()
                if status:
                    display_message += "\n\n# SENSOR CORE STATUS\n"
                    for key, value in status.items():
                        # Left pad the key to 24 characters
                        display_message += f"  {key:<24} {value}\n"

        # Get the device health
        health = DeviceHealth().get_health()

        if health:
            display_message += "\n\n# DEVICE HEALTH\n"
            for key, value in health.items():
                # Left pad the key to 24 characters
                display_message += f"  {key:<24} {value}\n"

        return display_message

    def display_configuration(self) -> str:
        """
        Display the current configuration of the RpiCore.

        Return:
        - A string message containing the configuration of the RpiCore.
        """
        display_message = f"\nConfiguration:\n{root_cfg.my_device.display()}"

        # Display the storage account name
        if root_cfg.keys:
            display_message += f"\nStorage account: {root_cfg.keys.get_storage_account()}\n"

        return display_message

    def _is_running(self)-> bool:
        """Check if an instance of RpiCore is running."""
        return EdgeOrchestrator.watchdog_file_alive()

    @staticmethod
    def _is_configured() -> bool:
        """Check if RpiCore is configured."""
        # Test for the presence of the SC_CONFIG_FILE file
        return root_cfg.SYSTEM_CFG_FILE.exists()

    @staticmethod    
    def update_my_device_id(new_device_id: str) -> None:
        """Function used in testing to change the device_id"""
        root_cfg.update_my_device_id(new_device_id)
