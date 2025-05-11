###############################################################################
# The run_my_sensor script is invoked by the SensorCore at startup.
# It provides a means for users to customize the behavior of the SensorCore
# and run their own code.
#
# By default, it:
# - loads the fleet configuration specified in system_cfg.my_fleet_config
# - starts the SensorCore
#################################################################################
from time import sleep

from sensor_core import SensorCore
from sensor_core import configuration as root_cfg

logger = root_cfg.setup_logger("bee_ops")

def main():
    """Run SensorCore as defined in the system.cfg file."""

    try:
        # Configure the SensorCore with the fleet configuration
        # This will load the configuration and check for errors
        logger.info("Creating SensorCore...")
        sc = SensorCore()

        # Load_configuration loads the configuration specified in system_cfg.my_fleet_config
        logger.info("Configuring SensorCore...")
        inventory = root_cfg.load_configuration()
        if inventory is None:
            logger.error("Failed to load inventory. Exiting...")
            return
        
        sc.configure(inventory)

        # Start the SensorCore and begin data collection
        logger.info("Starting SensorCore...")
        sc.start()
        while True:
            logger.info(sc.status())
            sleep(1800)

    except KeyboardInterrupt:
        logger.error("Keyboard interrupt => stopping SensorCore... this may take up to 180s.")
        sc.stop()
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sc.stop()


if __name__ == "__main__":
    main()