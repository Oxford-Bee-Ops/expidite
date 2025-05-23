
import logging

import pytest

from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite", logging.DEBUG)

class Test_bcli:
    @pytest.mark.unittest
    def test_bcli(self) -> None:

        # This is an interactive CLI.
        # Enter the value '7' to exit the CLI.
        # The CLI will then exit and the test will complete.
        # Simulate user input by patching 'input' to return '7'
        #with patch("click.prompt", side_effect=["7"]):
        #    bcli.main()

        # The CLI will exit after receiving '7', and the test will complete.
        pass