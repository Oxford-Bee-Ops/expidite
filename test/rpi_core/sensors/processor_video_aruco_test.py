import logging
import sys
from pathlib import Path

import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.sensors.processor_video_aruco import DEFAULT_AUROCO_PROCESSOR_CFG, VideoArucoProcessor

logger = root_cfg.setup_logger("expidite")


class Test_video_aruco_processor:
    logger.setLevel(level=logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    @pytest.mark.unittest
    def test_aruco_processor_basic(self) -> None:
        logger.info("Run test_aruco_processor_basic test")
        src_dir = Path(__file__).parent / "resources"
        file = (
            # src_dir / "V3_TRAPCAM_88a29e5945fd_00_00_20260321T114558780_20260321T114624905.mp4"
            src_dir / "V3_TRAPCAM_88a29e5945fd_00_00_20260324T140945267_20260324T140954267.mp4"
        ).resolve()

        # Run the processor
        processor = VideoArucoProcessor(config=DEFAULT_AUROCO_PROCESSOR_CFG, sensor_index=0)
        df = processor.process_video_file(source_file=file, aruco_dict_name="DICT_4X4_100")

        df = df[df["marker_id"] > 0]
        df.to_csv(root_cfg.TMP_DIR / "test_aruco_processor_basic_output.csv", index=False)

        print("##############################################################")
        print(f"# Found {len(df)} markers in the video")
        # Print the marker IDs as a list
        print(f"# Marker IDs: {df['marker_id'].tolist()}")
        print("##############################################################")
