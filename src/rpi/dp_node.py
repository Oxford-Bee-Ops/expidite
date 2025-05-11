import shutil
import threading  # Add this import for thread safety
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from random import random
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from sensor_core import api, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.dp_config_objects import DPtreeNodeCfg, Stream
from sensor_core.utils.journal_pool import JournalPool
from sensor_core.utils.sc_test_emulator import ScEmulator

logger = root_cfg.setup_logger("sensor_core")


@dataclass
class DPnodeStat:
    count: int = 0
    sum: float = 0.0
    def record(self, value: float) -> None:
        """Record a sample."""
        self.count += 1
        self.sum += value

class DPnode():
    """Base class for nodes in the DPtree. Sensor and DataProcessor inherit from this class.
    """
    # Special Datastream for recording sample count / duration from the data pipeline.
    _selftracker: "DPnode"

    def __init__(self, config: DPtreeNodeCfg, sensor_index: int) -> None:
        """
        Initializes a DPtreeNode with the given configuration.

        Args:
            config: The configuration object for this node, which can be a Sensor, DataProcessor, or
                    Datastream.
        """
        self._dpnode_config: DPtreeNodeCfg = config
        self.sensor_index: int = sensor_index
        self.cc: Optional[CloudConnector] = None

        self._dpnode_children: dict[int, DPnode] = {}  # Dictionary mapping output streams to child nodes.

        # Record the number of datapoints recorded by this Datastream (by type_id).
        self._dpnode_score_stats: dict[str, DPnodeStat] = {}
        # Record the duration of each DataProcessor cycle (by type_id).
        self._dpnode_scorp_stats: dict[str, DPnodeStat] = {}
        # Lock to ensure thread safety when accessing the stats dictionary.
        self._stats_lock = threading.Lock()  

        # Create the Journals that we will use to store this DPtree's output.
        self.journal_pool: Optional[JournalPool] = None


    def is_leaf(self, stream_index: int) -> bool:
        """Check if this node is a leaf node (i.e., it has no children).

        Returns:
            True if this node is a leaf node, False otherwise.
        """
        # Although the node may have children, we care about whether there is a child node for the
        # given stream_index. If there is no child node for this stream, is_leaf == True.
        return stream_index not in self._dpnode_children

    def get_config(self) -> DPtreeNodeCfg:
        """Return the configuration for this node."""
        return self._dpnode_config

    def get_stream(self, stream_index: int) -> Stream:
        """Return the Stream object for the given stream index.

        Args:
            stream_index: The index of the stream to retrieve.

        Returns:
            The Stream object for the specified stream index.

        Raises:
            ValueError: If the stream index is out of range for this node's configuration.
        """
        outputs = self._dpnode_config.outputs
        if outputs is None:
            raise ValueError(f"Outputs are not defined for {self._dpnode_config}.")
        if len(outputs) <= stream_index:
            raise ValueError(f"Stream index {stream_index} is out of range for {self._dpnode_config}.")
        return outputs[stream_index]

    def get_data_id(self, stream_index: int) -> str:
        """Return the data ID for the specified stream.

        Args:
            stream_index: The index of the requested stream.

        Returns:
            The data ID for this stream.
        """
        return self.get_stream(stream_index).get_data_id(self.sensor_index)

    def export(self) -> dict:
        """Export the configuration of this node and all its descendants as a dictionary.

        Returns:
            A dictionary representing the configuration.
            The keys in the dictionary are the indices used in Connect() to connect the nodes.
        """
        # Export the configuration of this node as a dictionary.
        # The config is a dataclass, so we can use the __dict__ attribute to get the fields.
        cfg_export: dict[str|int, dict] = {}
        cfg_export["node_cfg"] = asdict(self.get_config())
        cfg_export["children"] = {}
        for child_index, child in self._dpnode_children.items():
            # Recursively export the configuration of each child node.
            cfg_export["children"][child_index] = child.export()
        return cfg_export

    #########################################################################################################
    #
    # Public methods called by Sensor or DataProcessor to log data or save recordings.
    #
    #########################################################################################################
    def log(self, stream_index: int, sensor_data: dict) -> None:
        """Called by Sensor/DataProcessor to log a single 'row' of Sensor-generated data."""
        stream = self.get_stream(stream_index)
        data_id = self.get_data_id(stream_index)

        logger.debug(f"Log sensor_data: {sensor_data} to DPnode:{data_id} stream {stream_index}")

        # Check that the fields defined for this DatastreamType are present in the sensor_data
        # If any fields are missing, raise an exception
        log_data = {}
        assert stream.fields is not None, f"fields must be set in {stream} if logging data"
        for field in stream.fields:
            if field in api.REQD_RECORD_ID_FIELDS:
                continue
            elif field in sensor_data:
                log_data[field] = sensor_data[field]
            else:
                raise Exception(
                    f"Field {field} missing from data logged to {data_id}; "
                    f"Expected:{stream.fields}; "
                    f"Received the following fields:{sensor_data.keys()}"
                )

        # Add the Datastream indices (datastream_type_id, device_id, sensor_id) and a
        # timestamp to the log_data
        log_data[api.RECORD_ID.VERSION.value] = "V3"
        log_data[api.RECORD_ID.DATA_TYPE_ID.value] = stream.type_id
        log_data[api.RECORD_ID.DEVICE_ID.value] = root_cfg.my_device_id
        log_data[api.RECORD_ID.SENSOR_INDEX.value] = self.sensor_index
        log_data[api.RECORD_ID.STREAM_INDEX.value] = stream.index
        log_data[api.RECORD_ID.TIMESTAMP.value] = api.utc_to_iso_str()
        log_data[api.RECORD_ID.NAME.value] = root_cfg.my_device.name

        self._get_cpool().add_rows(stream, [log_data], api.utc_now())

        # Track the number of measurements recorded
        with self._stats_lock:
            self._dpnode_score_stats.setdefault(stream.type_id, DPnodeStat()).record(1)

        # We also spam the data to the logger for easy debugging and display in the bcli
        if stream.type_id not in api.SYSTEM_DS_TYPES:
            # We use the TELEM_TAG so that the BCLI can identify these as sensor logs for display.
            logger.info(f"{api.TELEM_TAG}Save log: {log_data!s}")
        else:
            logger.debug(f"Save log: {log_data!s}")


    def save_data(self, stream_index: int, sensor_data: pd.DataFrame) -> None:
        """Called by Sensors to save 1 or more 'rows' of Sensor-generated data.

        save_data() is used to save Pandas dataframes to the Stream.
        The 'format' field of the Stream object must be set to df or csv for this to be used.
        """
        if sensor_data.empty:
            logger.debug(f"Dataframe empty for {self.get_data_id(stream_index)}")
            return
        
        stream = self.get_stream(stream_index)
        sensor_data = self._validate_output(sensor_data, stream)
        self._get_cpool().add_rows_from_df(stream, sensor_data)

        # Track the number of measurements recorded
        # These data points don't have a duration - that only applies to recordings.
        with self._stats_lock:
            self._dpnode_score_stats.setdefault(stream.type_id, DPnodeStat()).record(len(sensor_data))
        logger.debug(f"Saved {len(sensor_data)} rows to {self.get_data_id(stream_index)}")


    def save_recording(
        self,
        stream_index: int,
        temporary_file: Path,
        start_time: datetime,
        end_time: Optional[datetime] = None,
    ) -> Path:
        """Called by a Sensor or DataProcessor to save a recording file to the appropriate datastore.
        This should only be used by Sensors or **primary** datastreams.

        Note: save_recording() will *rename* (ie move) the supplied temporary_file.
        This method will manage storage and subsequent processing of the temporary_file
        in line with the definition of this DatastreamType.
        The file name of the saved recording will be as per the naming convention defined in 
        Datastream.parse_filename().
        Do not use to save dataframes - see Datastream.save_data().

        Parameters
        ----------
        temporary_file: Path
            The path to the file that should be saved.
        start_time: datetime
            The time that the recording started.
        end_time:datetime
            Tthe time that the recording ended.
        """

        # If on EDGE, files are either saved to the root_cfg.EDGE_PROCESSING_DIR if there are DPs registered,
        # or to the root_cfg.EDGE_UPLOAD_DIR if not.
        # On the ETL, files are saved to the root_cfg.ETL_PROCESSING_DIR
        if root_cfg.get_mode() == root_cfg.Mode.EDGE:
            if self.is_leaf(stream_index):
                save_dir = root_cfg.EDGE_UPLOAD_DIR
            else:
                save_dir = root_cfg.EDGE_PROCESSING_DIR
        else:
            assert False, "save_recording() should not be called in ETL mode"

        new_fname = self._save_recording(
            stream_index=stream_index,
            src_file=temporary_file,
            dst_dir=save_dir,
            start_time=start_time,
            suffix=self.get_stream(stream_index).format,
            end_time=end_time,
        )
        # Logged in _save_recording()

        return new_fname


    def save_sub_recording(
        self,
        stream_index: int,
        temporary_file: Path,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        offset_index: Optional[int] = None,
        secondary_offset_index: Optional[int] = None,
    ) -> Path:
        """Called by DataProcessors to save sub-sample recording files to the appropriate datastore.
        Note: save_sub_recording() will *rename* (ie delete) the supplied temporary_file
        This method will manage storage and subsequent processing of the temporary_file
        in line with the definition of this DatastreamType.
        The file name of the saved recording will be as per the naming convention defined in 
        Datastream.parse_filename().
        Do not use to save dataframes - see Datastream.save_data().

        Parameters
        ----------
        data_processor: DataProcessor
            The DataProcessor object that is saving the file.
        temporary_file: Path
            The path to the file that should be saved.
        start_time: datetime
            The time that the recording started.
        end_time:datetime
            The time that the recording ended.
        offset_index: optional int
            Typically a frame number in the recording, if applicable.
        secondary_offset_index: optional int
            An index that can be used to differentiate between multiple subsamples from a given frame.
        """
        suffix = self.get_stream(stream_index).format

        # We save the recording to the EDGE|ETL_PROCESSING_DIR if there are more DPs to run, 
        # otherwise we save it to the EDGE|ETL_UPLOAD_DIR
        if root_cfg.get_mode() == root_cfg.Mode.EDGE:
            if self.is_leaf(stream_index):
                save_dir = root_cfg.EDGE_UPLOAD_DIR
            else:
                save_dir = root_cfg.EDGE_PROCESSING_DIR
        else:
            if self.is_leaf(stream_index):
                save_dir = root_cfg.ETL_PROCESSING_DIR
            else:
                save_dir = root_cfg.ETL_ARCHIVE_DIR

        new_fname = self._save_recording(
            stream_index=stream_index,
            src_file=temporary_file,
            dst_dir=save_dir,
            start_time=start_time,
            suffix=suffix,
            end_time=end_time,
            offset_index=offset_index,
            secondary_offset_index=secondary_offset_index,
        )
        # Logged in _save_recording()

        return new_fname

    
    def log_sample_data(self, sample_period_start_time: datetime) -> None:
        """Provide the count & duration of data samples recorded (environmental, media, etc)
        since the last time log_sample_data was called.

        This is used by EdgeOrchestrator to periodically log observability data
        """
        if DPnode._selftracker is None:
            logger.warning(f"{root_cfg.RAISE_WARN}SelfTracker not set; cannot log sample data")
            return

        # Lock the dictionary to prevent concurrent access
        with self._stats_lock:
            # Grab the data and release the lock.
            # Don't call selftracker.log inside the lock, as it may take a while to complete.
            score_stats: list[tuple[str, DPnodeStat]] = list(self._dpnode_score_stats.items())
            for type_id in self._dpnode_score_stats.keys():
                self._dpnode_score_stats[type_id] = DPnodeStat()
            scorp_stats: list[tuple[str, DPnodeStat]] = list(self._dpnode_score_stats.items())
            for type_id in self._dpnode_scorp_stats.keys():
                self._dpnode_scorp_stats[type_id] = DPnodeStat()


        # Log SCORE data
        for type_id, stat in score_stats:
            DPnode._selftracker.log(
                stream_index=api.SCORE_STREAM_INDEX,
                sensor_data={
                    "observed_type_id": type_id,
                    "observed_sensor_index": self.sensor_index,
                    "sample_period": api.utc_to_iso_str(sample_period_start_time),
                    "count": str(stat.count),
                }
            )

        # Log SCORP data
        for type_id, stat in scorp_stats:
            DPnode._selftracker.log(
                stream_index=api.SCORP_STREAM_INDEX,
                sensor_data={
                    # The data_processor_id is the subclass name of this object
                    "data_processor_id": self.__class__.__name__,
                    "observed_type_id": type_id,
                    "observed_sensor_index": self.sensor_index,
                    "sample_period": api.utc_to_iso_str(sample_period_start_time),
                    "count": str(stat.count),
                    "duration": str(stat.sum),
                }
            )
        logger.debug("Logged sample data for SCORE & SCORP")

    
    def save_sample(self, sample_probability: str | None) -> bool:
        """Return True if this node should save sample data to the datastore.
        This method can be subclassed to provide more complex sampling logic.
        In this default implementation, we assume sample_probability is a
        float value between 0.0 and 1.0."""
        if sample_probability is None:
            return False
        try:
            prob = float(sample_probability)
            if prob < 0.0 or prob > 1.0:
                raise ValueError(f"Invalid sample probability: {sample_probability}; "
                                 f"expected a value between 0.0 and 1.0")
        except ValueError:
            raise ValueError(f"Invalid sample probability: {sample_probability}; "
                             f"expected a value between 0.0 and 1.0")
        
        return random() < prob
    

    #########################################################################################################
    #
    # Private methods in support of Sensors
    #
    #########################################################################################################
    def _scorp_stat(self, stream_index: int, duration: float) -> None:
        """Record the duration of a DataProcessor cycle in the SCORP stream."""
        stream = self.get_stream(stream_index)
        with self._stats_lock:
            self._dpnode_scorp_stats.setdefault(stream.type_id, DPnodeStat()).record(duration)
        logger.debug(f"Recorded SCORP stat for {stream.type_id} duration {duration}")

    def _save_recording(
        self,
        stream_index: int,
        src_file: Path,
        dst_dir: Path,
        start_time: datetime,
        suffix: api.FORMAT,
        end_time: Optional[datetime] = None,
        offset_index: Optional[int] = None,
        secondary_offset_index: Optional[int] = None,
    ) -> Path:
        """Private method that handles saving of recordings from Datastreams or DataProcessors.

        Parameters
        ----------
        temporary_file: Path
            The path to the file that should be saved.
        start_time: datetime
            The time that the recording started.
        suffix: str
            The file extension of the recording.
        end_time:datetime
            Tthe time that the recording ended.
        offset_index: optional int
            Typically a frame number in the recording, if applicable.
        secondary_offset_index: optional int
            An index that can be used to differentiate between multiple subsamples from a given frame.
        """
        stream = self.get_stream(stream_index)
        data_id = stream.get_data_id(self.sensor_index)

        # Check that the file is present and not empty
        if not src_file.exists():
            raise FileNotFoundError(f"File {src_file} not found.")

        # Check that the file is of the correct format.
        # This should match the suffix provided.
        if not src_file.suffix.endswith(suffix.value):
            raise ValueError(f"File format {src_file.suffix} does not match expected suffix {suffix}.")
        
        # Check that the start_time and end_time are valid
        if not isinstance(start_time, datetime):
            raise ValueError("Start_time must be a valid datetime object.")
        if end_time is not None:
            if not isinstance(end_time, datetime):
                raise ValueError("End_time must be a valid datetime object.")
            
        # Check that the start_time and end_time are both timezone aware
        if start_time.tzinfo is None:
            logger.warning(f"{root_cfg.RAISE_WARN}start_time must be timezone aware. "
                           "Use api.utc_now() to get the current time.")
            start_time = start_time.replace(tzinfo=ZoneInfo("UTC"))
        if end_time is not None and end_time.tzinfo is None:
            logger.warning(f"{root_cfg.RAISE_WARN}end_time must be timezone aware. "
                           "Use api.utc_now() to get the current time.")
            end_time = end_time.replace(tzinfo=ZoneInfo("UTC"))

        if end_time is not None:
            if start_time > end_time:
                raise ValueError(f"Start_time ({start_time}) must be before end_time ({end_time}).")

        # Generate the filename for the recording
        new_fname: Path = file_naming.get_record_filename(
            dst_dir, 
            data_id,
            suffix, 
            start_time, 
            end_time, 
            offset_index, 
            secondary_offset_index
        )

        # If we're in test mode, we may cap the number of recordings we save.
        if root_cfg.TEST_MODE == root_cfg.MODE.TEST:
            if not ScEmulator.get_instance().ok_to_save_recording(stream.type_id):
                logger.info(f"Test mode recording cap hit; deleting {src_file.name}")
                if src_file.exists():
                    src_file.unlink()
                return new_fname

        # Move the file to the dst_dir (EDGE_UPLOAD_DIR or EDGE_PROCESSING_DIR)
        # This will be the first step in the processing of the file
        # After processing, the file will be moved to the appropriate datastore
        if new_fname != src_file:
            if new_fname.exists():
                # Increment the new_fname to avoid overwriting existing files
                new_fname = file_naming.increment_filename(new_fname)
            new_fname = src_file.rename(new_fname)

        if self.save_sample(stream.sample_probability) and stream.cloud_container is not None:
            # Generate a *copy* of the raw sample file because the original is in the Processing directory
            # and may soon by picked up by a DataProcessor.
            # The filename is the same as the recording, but saved to the upload directory
            if new_fname.parent == root_cfg.EDGE_UPLOAD_DIR:
                logger.warning(f"All recordings are being saved, but we're also saving samples."
                               f" Config error in {self.get_data_id(stream_index)} config?")
                
            sample_fname = file_naming.increment_filename(root_cfg.EDGE_UPLOAD_DIR / new_fname.name)
            shutil.copy(new_fname, sample_fname)
            self._get_cc().upload_to_container(stream.cloud_container,
                                                [sample_fname], 
                                                delete_src=True,
                                                storage_tier=stream.storage_tier)
            logger.info(f"Raw sample saved to {stream.cloud_container}; "
                        f"sample_prob={stream.sample_probability}")


        # If the dst_dir is EDGE_UPLOAD_DIR, we can use direct upload to the cloud
        if dst_dir == root_cfg.EDGE_UPLOAD_DIR:
            stream = self.get_stream(stream_index)
            assert stream.cloud_container is not None and stream.storage_tier is not None
            self._get_cc().upload_to_container(stream.cloud_container, 
                                                [new_fname], 
                                                delete_src=True,
                                                storage_tier=stream.storage_tier)

        # Track the number of measurements recorded
        with self._stats_lock:
            if end_time is None:
                self._dpnode_score_stats.setdefault(stream.type_id, DPnodeStat()).record(1)
            else:
                # Track duration if this file represents a period
                self._dpnode_score_stats.setdefault(stream.type_id, DPnodeStat()).record(
                    (end_time - start_time).total_seconds())

        logger.debug(f"Saved recording {src_file.name} as {new_fname.name}")

        return new_fname

    #########################################################################################################
    #
    # Private methods used in support of DataProcessors
    #
    #########################################################################################################
    def _validate_output(self, output_data: pd.DataFrame, stream: Stream) -> pd.DataFrame:
        if output_data is None or output_data.empty:
            return output_data

        data_id = stream.get_data_id(self.sensor_index)

        # Output DFs must always contain the core RECORD_ID fields
        # If not already present, add the RECORD_ID fields to the output_df
        for field in api.REQD_RECORD_ID_FIELDS:
            if field not in output_data.columns:
                if field == api.RECORD_ID.VERSION.value:
                    output_data[field] = "V3"
                elif field == api.RECORD_ID.TIMESTAMP.value:
                    output_data[field] = api.utc_to_iso_str()
                elif field == api.RECORD_ID.DEVICE_ID.value:
                    output_data[field] = root_cfg.my_device_id
                elif field == api.RECORD_ID.SENSOR_INDEX.value:
                    output_data[field] = self.sensor_index
                elif field == api.RECORD_ID.DATA_TYPE_ID.value:
                    output_data[field] = stream.type_id
                elif field == api.RECORD_ID.STREAM_INDEX.value:
                    output_data[field] = stream.index
                elif field == api.RECORD_ID.NAME.value:
                   output_data[field] = root_cfg.my_device.name
                else:
                    assert False, f"Unknown RECORD_ID field {field}"    
        # Check the values in the RECORD_ID are not nan or empty
        for field in api.REQD_RECORD_ID_FIELDS:
            if not output_data[field].notna().all():
                err_str = (f"{root_cfg.RAISE_WARN()}{field} contains NaN or empty values in output df"
                           f" {data_id}")
                logger.error(err_str)
                raise Exception(err_str)

        # Warn about superfluous fields that will get dropped
        if stream.fields is not None and len(stream.fields) > 0:
            for field in output_data.columns:
                if (
                    (stream.fields is not None)
                    and (field not in stream.fields)
                    and (field not in api.ALL_RECORD_ID_FIELDS)
                ):
                    logger.warning(
                        f"{field} in output from {data_id} "
                        f"but not in defined fields: {stream.fields}"
                    )

            # Output DF should contain the fields defined by the DP's output_fields list.
            for field in stream.fields:
                if field not in output_data.columns:
                    err_str = (f"{root_cfg.RAISE_WARN()}{field} missing from output_df on "
                            f"{data_id}: {output_data.columns}")
                    logger.error(err_str)
                    raise Exception(err_str)

        return output_data

    ##########################################################################################################
    # Utilities
    ##########################################################################################################
    def _get_cc(self) -> CloudConnector:
        """Return the CloudConnector object for this node.
        We don't do this during init to avoid unnecessary work for things like config validation that
        don't need the CloudConnector.
        """
        if self.cc is None:
            self.cc = CloudConnector.get_instance(root_cfg.CLOUD_TYPE)
        return self.cc
    
    def _get_cpool(self) -> JournalPool:
        """Return the JournalPool object for this node.
        We don't do this during init to avoid unnecessary work for things like config validation that
        don't need the JournalPool."""
        if self.journal_pool is None:
            self.journal_pool = JournalPool.get(mode=root_cfg.get_mode())
        return self.journal_pool