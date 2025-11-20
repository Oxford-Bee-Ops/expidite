import logging
import sys

import pytest

from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")


class Test_video_aruco_processor:
    logger.setLevel(level=logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    @pytest.mark.unittest
    def test_aruco_processor_basic(self):
        logger.info("Run test_aruco_processor_basic test")
        pass
        #file = (
        #    root_cfg.CODE_DIR / "rpi_core"
        #    / "test"
        #    / "sensors"
        #    / "resources"
        #    / "5fps_4X4 5Mm 30Cm 20250107 154846.mp4"
        #)

        # Run the processor
        # @@@@ need a test harness for processors
        #processor = processor_video_aruco.VideoArucoProcessor()
        #processor.process_video_file(source_file=file)
