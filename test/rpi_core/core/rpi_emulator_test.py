import logging

import pytest
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.utils import rpi_emulator

logger = root_cfg.setup_logger("expidite", logging.DEBUG)

class Test_rpi_emulator:
    @pytest.mark.unittest
    def test_rpi_emulator(self) -> None:
        with rpi_emulator.RpiEmulator.get_instance() as th:
            # Limit the RpiCore to 1 recording so we can easily validate the results
            th.set_recording_cap(1, type_id="test")

            result = th.ok_to_save_recording("test")
            assert result is True, "Expected ok_to_save_recording to return True"

            result = th.ok_to_save_recording("test")
            assert result is False, "Expected ok_to_save_recording to return False"

