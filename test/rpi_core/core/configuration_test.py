
import pytest
from expidite_rpi.core import config_validator
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.example import my_fleet_config

logger = root_cfg.setup_logger("expidite")
root_cfg.TEST_MODE = root_cfg.MODE.TEST

class Test_configuration:
    @pytest.mark.parametrize(
        "test_input,expected",
        [
            ("('d01111111111','name')", "DUMMY"),
        ],
    )
    @pytest.mark.unittest
    def test_get_field(self, test_input: str, expected: str) -> None:
        logger.info("Run test_get_field test")
        _, key = eval(test_input)
        assert root_cfg.my_device.get_field(key) == expected

    @pytest.mark.unittest
    def test_display_cfg(self) -> None:
        logger.info("Run test_display_cfg test")
        assert root_cfg.my_device.display() != ""

    @pytest.mark.unittest
    def test_config_validator(self) -> None:
        logger.info("Run test_config_validator test")
        # Check the configuration is valid
        dptrees = my_fleet_config.create_example_device()
        is_valid, error_message = config_validator.validate_trees(dptrees)
        assert is_valid, error_message
