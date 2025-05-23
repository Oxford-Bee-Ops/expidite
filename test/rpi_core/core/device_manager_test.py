import pytest
from expidite_rpi.core import device_manager

from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

class Test_device_manager:
    @pytest.mark.unittest
    def test_device_manager(self) -> None:
        logger.info("Run test_device_manager test")
        device_manager.DeviceManager()