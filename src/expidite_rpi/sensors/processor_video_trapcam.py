from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import cv2
import pandas as pd

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp import DataProcessor
from expidite_rpi.core.dp_config_objects import DataProcessorCfg, Stream

logger = root_cfg.setup_logger("expidite")

TRAPCAM_TYPE_ID = "TRAPCAM"
TRAPCAM_STREAM_INDEX: int = 0
TRAPCAM_STREAM: Stream = Stream(
    description="Video samples with movement detected.",
    type_id=TRAPCAM_TYPE_ID,
    index=TRAPCAM_STREAM_INDEX,
    format=api.FORMAT.MP4,
    cloud_container="expidite-upload",
    sample_probability="1.0",
)


@dataclass
class TrapcamDpCfg(DataProcessorCfg):
    ##########################################################################################################
    # Add custom fields
    ##########################################################################################################
    min_blob_size: int = 1000  # Minimum blob size in pixels
    max_blob_size: int = 1000000  # Maximum blob size in pixels
    padding_seconds: float = 1.0  # Padding either side of detected movement in seconds


DEFAULT_TRAPCAM_DP_CFG = TrapcamDpCfg(
    description="Video processor that detects movement in video files and saves segments with movement.",
    outputs=[TRAPCAM_STREAM],
)


class TrapcamDp(DataProcessor):
    """Simple DP that processes video files from a camera, detects movement, and saves
    video segments with movement.
    """

    def __init__(self, config: TrapcamDpCfg, sensor_index: int) -> None:
        super().__init__(config, sensor_index)
        self.config: TrapcamDpCfg = config
        self.kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
        self.background_subtractor = cv2.createBackgroundSubtractorMOG2()

    def process_data(
        self,
        input_data: pd.DataFrame | list[Path],
    ) -> None:
        """Process a list of video files and resave video segments with movement."""
        assert isinstance(input_data, list), f"Expected list of files, got {type(input_data)}"
        files: list[Path] = input_data  # type: ignore[invalid-assignment]
        min_blob_size = self.config.min_blob_size
        max_blob_size = self.config.max_blob_size

        for f in files:
            try:
                logger.info(f"Processing video file: {f!s}")
                self.process_video_new(f, min_blob_size, max_blob_size, self.config.padding_seconds)
            except Exception:
                logger.exception(f"{root_cfg.RAISE_WARN()}Exception occurred processing video {f!s}")

    def process_video(
        self, video_path: Path, min_blob_size: int, max_blob_size: int, padding_seconds: float
    ) -> None:
        """Process a video file to detect movement and save segments with movement.
        We record for a minimum of `padding_seconds` seconds before and after movement is detected.
        """
        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            exists = video_path.exists()
            msg = f"Unable to open video file (exists={exists}): {video_path}; opencv installation issue?"
            raise ValueError(msg)

        fname_details = file_naming.parse_record_filename(video_path)
        start_time = fname_details[api.RECORD_ID.TIMESTAMP.value]
        frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        suffix = video_path.suffix[1:]
        if suffix == "h264":
            fourcc = cv2.VideoWriter.fourcc(*"h264")
        elif suffix == "mp4":
            fourcc = cv2.VideoWriter.fourcc(*"mp4v")
        else:
            msg = f"Unsupported video format: {suffix}"
            raise ValueError(msg)

        samples_saved = 0
        sum_sample_duration = 0
        output_stream: cv2.VideoWriter | None = None
        current_frame = -1
        sample_first_frame = 0
        sample_last_movement_frame = 0
        frames_to_record = int(fps * padding_seconds)  # Record for some time after movement is detected
        discard_threshold = 2  # Discard the recording if it was just noise; ie less than X frames
        temp_filename: Path = Path("unspecified")  # Set when we start saving video

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

            if movement and (current_frame > 1):  # Ignore the first 2 frames while the BS settles
                if not output_stream:
                    # Not currently recording; start recording
                    sample_first_frame = current_frame
                    sample_last_movement_frame = current_frame
                    temp_filename = file_naming.get_temporary_filename(api.FORMAT.MP4)
                    output_stream = cv2.VideoWriter(
                        filename=str(temp_filename),
                        fourcc=fourcc,
                        fps=fps,
                        frameSize=(frame_width, frame_height),
                    )
                    output_stream.write(frame)
                else:
                    # Already recording; update the last movement frame ID
                    sample_last_movement_frame = current_frame
                    output_stream.write(frame)
            # No movement detected...
            elif output_stream:
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
                        logger.info(
                            f"Saving video of {sample_duration}s starting {sample_start_time} to {self}"
                        )
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
                        logger.info(
                            f"Discarding {(sample_last_movement_frame - sample_first_frame)}"
                            f" frames of movement as noise"
                        )
                        temp_filename.unlink(missing_ok=True)

        logger.info(f"Saved {samples_saved} samples ({sum_sample_duration}s) from video: {video_path}")
        cap.release()

    def process_video_new(
        self, video_path: Path, min_blob_size: int, max_blob_size: int, padding_seconds: float
    ) -> None:
        """Process a video file using a 3-phase approach:
        1. Identify all frames with qualifying levels of movement
        2. Optimize video segments with 1 second padding before/after
        3. Write out the optimized video segments.
        """
        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            exists = video_path.exists()
            msg = f"Unable to open video file (exists={exists}): {video_path}; opencv installation issue?"
            raise ValueError(msg)

        try:
            fname_details = file_naming.parse_record_filename(video_path)
            start_time = fname_details[api.RECORD_ID.TIMESTAMP.value]
            frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            fps = int(cap.get(cv2.CAP_PROP_FPS))
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

            if fps == 0:
                msg = f"FPS is 0 for video {video_path}; cannot process"
                raise ValueError(msg)

            suffix = video_path.suffix[1:]
            if suffix == "h264":
                fourcc = cv2.VideoWriter.fourcc(*"h264")
            elif suffix == "mp4":
                fourcc = cv2.VideoWriter.fourcc(*"mp4v")
            else:
                msg = f"Unsupported video format: {suffix}"
                raise ValueError(msg)

            logger.info(
                f"Processing video (3-phase) with fps={fps}, frames={total_frames}, "
                f"res={frame_width}x{frame_height}: {video_path}"
            )

            # Reset background subtractor for each video
            self.background_subtractor = cv2.createBackgroundSubtractorMOG2()

            # PHASE 1: Identify all frames with movement
            logger.info("Phase 1: Scanning for movement frames")
            movement_frames = self._detect_movement_frames(cap, min_blob_size, max_blob_size, total_frames)

            if not movement_frames:
                logger.info("No movement detected in video")
                return

            # PHASE 2: Optimize video segments with padding
            logger.info(f"Phase 2: Optimizing {len(movement_frames)} movement frames into segments")
            padding_frames = int(fps * padding_seconds)  # Convert padding seconds to frames
            segments = self._optimize_segments(movement_frames, padding_frames, total_frames)

            logger.info(f"Created {len(segments)} optimized segments")

            # PHASE 3: Write out the videos
            logger.info("Phase 3: Writing video segments")
            samples_saved = self._write_video_segments(
                cap, video_path, segments, start_time, fps, fourcc, frame_width, frame_height
            )

            total_duration = sum((end - start) / fps for start, end in segments)
            logger.info(f"Saved {samples_saved} samples ({total_duration:.1f}s) from video: {video_path}")

        finally:
            cap.release()

    def _detect_movement_frames(
        self, cap: cv2.VideoCapture, min_blob_size: int, max_blob_size: int, total_frames: int
    ) -> list[int]:
        """Phase 1: Scan through video and identify frames with qualifying movement."""
        movement_frames = []
        current_frame = 0

        cap.set(cv2.CAP_PROP_POS_FRAMES, 0)  # Reset to beginning

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            # Skip first few frames while background subtractor initializes
            if current_frame > 1:
                fg_mask = self.background_subtractor.apply(frame)
                fg_mask = cv2.morphologyEx(fg_mask, cv2.MORPH_OPEN, self.kernel)
                contours, _ = cv2.findContours(fg_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

                # Check for qualifying movement
                for c in contours:
                    contour_area = cv2.contourArea(c)
                    if min_blob_size < contour_area < max_blob_size:
                        movement_frames.append(current_frame)
                        break
            else:
                # Still need to train background subtractor on initial frames
                self.background_subtractor.apply(frame)

            current_frame += 1

        return movement_frames

    def _optimize_segments(
        self, movement_frames: list[int], padding_frames: int, total_frames: int
    ) -> list[tuple[int, int]]:
        """Phase 2: Create optimized segments with padding, merging overlapping segments."""
        if not movement_frames:
            return []

        segments = []

        # Group consecutive movement frames into clusters
        clusters = []
        current_cluster = [movement_frames[0]]

        for frame in movement_frames[1:]:
            # If frame is close to the last frame in cluster (within reasonable gap)
            if frame - current_cluster[-1] <= padding_frames * 2:  # Allow gaps up to 2 seconds
                current_cluster.append(frame)
            else:
                # Start new cluster
                clusters.append(current_cluster)
                current_cluster = [frame]
        clusters.append(current_cluster)  # Add final cluster

        # Convert clusters to segments with padding
        for cluster in clusters:
            start_frame = max(0, cluster[0] - padding_frames)
            end_frame = min(total_frames - 1, cluster[-1] + padding_frames)
            segments.append((start_frame, end_frame))

        # Merge overlapping segments
        merged_segments: list[tuple[int, int]] = []
        for start, end in sorted(segments):
            if merged_segments and start <= merged_segments[-1][1]:
                # Overlapping segment, merge with previous
                merged_segments[-1] = (merged_segments[-1][0], max(merged_segments[-1][1], end))
            else:
                merged_segments.append((start, end))

        return merged_segments

    def _write_video_segments(
        self,
        cap: cv2.VideoCapture,
        video_path: Path,
        segments: list[tuple[int, int]],
        start_time: datetime,
        fps: int,
        fourcc: int,
        frame_width: int,
        frame_height: int,
    ) -> int:
        """Phase 3: Write out the optimized video segments."""
        samples_saved = 0

        for segment_idx, (start_frame, end_frame) in enumerate(segments):
            segment_duration = (end_frame - start_frame) / fps

            # Skip very short segments (less than 0.5 seconds)
            if segment_duration < 0.5:
                logger.debug(f"Skipping short segment {segment_idx}: {segment_duration:.2f}s")
                continue

            temp_filename = file_naming.get_temporary_filename(api.FORMAT.MP4)

            try:
                output_stream = cv2.VideoWriter(
                    filename=str(temp_filename),
                    fourcc=fourcc,
                    fps=fps,
                    frameSize=(frame_width, frame_height),
                )

                # Set video position to start of segment
                cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

                frames_written = 0
                for _ in range(start_frame, end_frame + 1):
                    ret, frame = cap.read()
                    if not ret:
                        break
                    output_stream.write(frame)
                    frames_written += 1

                output_stream.release()

                if frames_written > 0:
                    # Calculate timestamps for this segment
                    segment_start_time = start_time + timedelta(seconds=(start_frame / fps))
                    segment_end_time = start_time + timedelta(seconds=(end_frame / fps))

                    logger.info(
                        f"Saving segment {segment_idx}: {segment_duration:.1f}s from frame "
                        f"{start_frame}-{end_frame}"
                    )

                    self.save_recording(
                        stream_index=TRAPCAM_STREAM_INDEX,
                        temporary_file=temp_filename,
                        start_time=segment_start_time,
                        end_time=segment_end_time,
                    )
                    samples_saved += 1
                else:
                    # No frames written, clean up temp file
                    temp_filename.unlink(missing_ok=True)

            except Exception:
                logger.exception(f"Error writing segment {segment_idx}")
                temp_filename.unlink(missing_ok=True)

        return samples_saved
