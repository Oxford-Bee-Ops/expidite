from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Optional

import cv2
import pandas as pd

from sensor_core import api, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.dp import DataProcessor
from sensor_core.dp_config_objects import DataProcessorCfg, Stream

logger = root_cfg.setup_logger("sensor_core")

TRAPCAM_TYPE_ID = "TRAPCAM"
TRAPCAM_STREAM_INDEX: int = 0

@dataclass
class TrapCamProcessorCfg(DataProcessorCfg):
    ########################################################################
    # Add custom fields
    ########################################################################
    min_blob_size: int = 1000  # Minimum blob size in pixels
    max_blob_size: int = 1000000  # Maximum blob size in pixels

DEFAULT_TRAPCAM_PROCESSOR_CFG = TrapCamProcessorCfg(
    description="Video processor that detects movement in video files and saves segments with movement.",
    outputs=[
        Stream(
            description="Video samples with movement detected.",
            type_id=TRAPCAM_TYPE_ID,
            index=TRAPCAM_STREAM_INDEX,
            format=api.FORMAT.MP4,
            cloud_container="sensor-core-upload",
            sample_probability="0.1",
        )
    ],
    min_blob_size=1000,
    max_blob_size=1000000,
)

class ProcessorVideoTrapCam(DataProcessor):
    def __init__(self, config: TrapCamProcessorCfg, sensor_index: int) -> None:
        super().__init__(config, sensor_index)
        self.config: TrapCamProcessorCfg = config
        self.kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE,(3,3))
        self.background_subtractor = cv2.createBackgroundSubtractorMOG2()

    def process_data(
        self, 
        input_data: pd.DataFrame | list[Path],
    ) -> None:
        """Process a list of video files and resave video segments with movement."""

        assert isinstance(input_data, list), f"Expected list of files, got {type(input_data)}"
        files: list[Path] = input_data
        min_blob_size = self.config.min_blob_size
        max_blob_size = self.config.max_blob_size

        for f in files:
            try:
                logger.info(f"Processing video file: {f!s}")
                self.process_video(f, min_blob_size, max_blob_size)
            except Exception as e:
                logger.error(
                    f"{root_cfg.RAISE_WARN()}Exception occurred processing video {f!s}; {e!s}",
                    exc_info=True,
                )

    def process_video(self, 
                      video_path: Path, 
                      min_blob_size: int, 
                      max_blob_size: int) -> None:
        """ Process a video file to detect movement and save segments with movement. 
        We record for a minimum of 2 seconds after movement is detected."""

        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            exists = video_path.exists()
            raise ValueError(f"Unable to open video file (exists={exists}): {video_path};"
                             f" opencv installation issue?")

        fname_details = file_naming.parse_record_filename(video_path)
        start_time = fname_details[api.RECORD_ID.TIMESTAMP.value]
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        suffix = video_path.suffix[1:]
        if suffix == "h264":
            fourcc = cv2.VideoWriter.fourcc(*'h264')
        elif suffix == "mp4":
            fourcc = cv2.VideoWriter.fourcc(*'mp4v')
        else:
            raise ValueError(f"Unsupported video format: {suffix}")

        samples_saved = 0
        sum_sample_duration = 0
        output_stream: Optional[cv2.VideoWriter | None] = None
        current_frame = -1
        sample_first_frame = 0
        sample_last_movement_frame = 0
        frames_to_record = 1 * fps  # Record for some time after movement is detected
        discard_threshold = 2 # Discard the recording if it was just noise; ie less than X frames
        temp_filename: Path = Path("unspecified") # Set when we start saving video

        logger.info(f"Processing video with fps={fps}, res={frame_width}x{frame_height}: {video_path}")

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                # If were in the middle of recording and the video ends, stop saving
                if output_stream:
                    output_stream.release()
                    output_stream = None
                    sample_start_time = start_time + timedelta(seconds=(sample_first_frame / fps))
                    sample_end_time = start_time + timedelta(seconds=(current_frame / fps))
                    sample_duration = (sample_end_time - sample_start_time).total_seconds()
                    if (sample_last_movement_frame - sample_first_frame) > discard_threshold:
                        self.save_recording(
                            stream_index=TRAPCAM_STREAM_INDEX,
                            temporary_file=temp_filename,
                            start_time=sample_start_time,
                            end_time=sample_end_time,
                        )
                        samples_saved += 1
                        sum_sample_duration += sample_duration
                break

            current_frame += 1
            fg_mask = self.background_subtractor.apply(frame)
            fg_mask = cv2.morphologyEx(fg_mask, cv2.MORPH_OPEN, self.kernel)
            contours, _ = cv2.findContours(fg_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            movement = False
            for c in contours:
                contour_area = cv2.contourArea(c)
                if contour_area > min_blob_size and contour_area < max_blob_size:
                    movement = True
                    break

            if movement and (current_frame > 1): # Ignore the first 2 frames while the BS settles
                if not output_stream:
                    # Not currently recording; start recording
                    sample_first_frame = current_frame
                    sample_last_movement_frame = current_frame
                    temp_filename=file_naming.get_temporary_filename(api.FORMAT.MP4)
                    output_stream = cv2.VideoWriter(
                        filename=str(temp_filename),
                        fourcc=fourcc,
                        fps=fps,
                        frameSize=(frame_width, frame_height)
                    )
                    output_stream.write(frame)
                else:
                    # Already recording; update the last movement frame ID
                    sample_last_movement_frame = current_frame
                    output_stream.write(frame)
            else:
                # No movement detected... 
                if output_stream:
                    # ...but we are currently saving video
                    if (current_frame - sample_last_movement_frame) < frames_to_record:
                        # ...and we're still within the recording window
                        output_stream.write(frame)
                    else:
                        # No movement for a while, stop saving video
                        output_stream.release()
                        output_stream = None
                        sample_start_time = start_time + timedelta(seconds=(sample_first_frame / fps))
                        sample_end_time = start_time + timedelta(seconds=(current_frame / fps))

                        # Check if we have enough frames to save
                        sample_duration = (sample_end_time - sample_start_time).total_seconds()
                        if (sample_last_movement_frame - sample_first_frame) > discard_threshold:
                            # Save the video segment to the derived datastream
                            logger.info(f"Saving video of {sample_duration}s to {self}")
                            self.save_recording(
                                stream_index=TRAPCAM_STREAM_INDEX,
                                temporary_file=temp_filename,
                                start_time=sample_start_time,
                                end_time=sample_end_time,
                            )
                            samples_saved += 1
                            sum_sample_duration += sample_duration
                        else: 
                            # Discard the video segment
                            logger.info(f"Discarding {(sample_last_movement_frame - sample_first_frame)}"
                                        f" frames of movement as noise")
                            temp_filename.unlink(missing_ok=True)
                            
        logger.info(f"Saved {samples_saved} samples ({sum_sample_duration}s) from video: {video_path}")
        cap.release()

