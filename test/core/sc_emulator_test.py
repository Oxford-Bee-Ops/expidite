import logging

import pytest
from sensor_core import configuration as root_cfg
from sensor_core.utils import sc_test_emulator

logger = root_cfg.setup_logger("sensor_core", logging.DEBUG)

class Test_sc_emulator:
    @pytest.mark.quick
    def test_sc_emulator(self) -> None:
        with sc_test_emulator.ScEmulator.get_instance() as th:
            # Limit the SensorCore to 1 recording so we can easily validate the results
            th.set_recording_cap(1, type_id="test")

            result = th.ok_to_save_recording("test")
            assert result is True, "Expected ok_to_save_recording to return True"

            result = th.ok_to_save_recording("test")
            assert result is False, "Expected ok_to_save_recording to return False"

