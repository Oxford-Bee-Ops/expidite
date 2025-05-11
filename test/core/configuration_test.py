
import pytest
from example import my_fleet_config
from sensor_core import config_validator
from sensor_core import configuration as root_cfg

logger = root_cfg.setup_logger("sensor_core")
root_cfg.TEST_MODE = root_cfg.MODE.TEST

class Test_configuration:
    @pytest.mark.parametrize(
        "test_input,expected",
        [
            ("('d01111111111','name')", "DUMMY"),
        ],
    )
    @pytest.mark.quick
    def test_get_field(self, test_input: str, expected: str) -> None:
        _, key = eval(test_input)
        assert root_cfg.my_device.get_field(key) == expected

    @pytest.mark.quick
    def test_display_cfg(self) -> None:
        assert root_cfg.my_device.display() != ""

    @pytest.mark.quick
    def test_config_validator(self) -> None:
        # Check the configuration is valid
        dptrees = my_fleet_config.create_example_device()
        is_valid, error_message = config_validator.validate_trees(dptrees)
        assert is_valid, error_message
