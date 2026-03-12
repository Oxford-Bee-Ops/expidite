from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector.cloud_connector import CloudConnector

logger = root_cfg.setup_logger("expidite")


##############################################################################################################
# SyncCloudConnector class
#
# This class is simply the original CloudConnector with no subclassed methods.
# But we need it to be able to use the CloudConnector.get_instance() method.
##############################################################################################################
class SyncCloudConnector(CloudConnector):
    def __init__(self) -> None:
        logger.debug("Creating SyncCloudConnector instance")
        super().__init__()

    def shutdown(self) -> None:
        """Shutdown the SyncCloudConnector instance."""
        logger.debug("Shutting down SyncCloudConnector")
        super().shutdown()
