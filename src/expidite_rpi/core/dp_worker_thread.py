from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Event, Thread
from typing import Optional

import pandas as pd
import yaml
from yaml import Dumper

from expidite_rpi import DataProcessor, DPtree, SensorCfg, Stream, api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import CloudConnector
from expidite_rpi.core.configuration import Mode
from expidite_rpi.core.device_config_objects import Keys
from expidite_rpi.core.dp_node import DPnode

logger = root_cfg.setup_logger("expidite")


class DPworker(Thread):
    """A DPworker is the thread that processes data through a DPtree.
    Note: the Sensor has a separate thread.
    """
    _scorp_dp: DPnode

    def __init__(
        self,
        dp_tree: DPtree,
    ) -> None:
        """Initialise the DPworker."""
        # Set a custom thread name based on the sensor index
        thread_name = f"{dp_tree.sensor.config.sensor_type.value}-{dp_tree.sensor.config.sensor_index}"
        
        Thread.__init__(self, name=thread_name)
        logger.debug(f"Initialising DPworker {self}")

        self._stop_requested = Event()
        self.cc: CloudConnector = CloudConnector.get_instance(root_cfg.CLOUD_TYPE)

        # sensor_cfg is a SensorCfg object describing the sensor that produces this datastream.
        self.dp_tree: DPtree = dp_tree

        # device_id is the machine ID (mac address) of the device that produces this datastream.
        self.device_id = root_cfg.my_device_id

        # sensor_id is an index (eg port number) identifying the sensor that produces this datastream.
        # This is not unique on the device, but must be unique in combination with the datastream_type_id.
        self.sensor_index = dp_tree.sensor.config.sensor_index

        # start_time is a datetime object that describes the time the DPworker was started.
        # This should be set by calling the start() method, and not set during initialization.
        self.dpe_start_time: Optional[datetime] = None


    #########################################################################################################
    #
    # Public methods called by the Sensor or DataProcessor to log or save data
    #
    #########################################################################################################

    def save_FAIR_record(self) -> None:
        """Save a FAIR record describing this Sensor and associated data processing to the FAIR archive."""
        logger.debug(f"Save FAIR record for {self}")

        # Custom representer for Enum
        def enum_representer(dumper: Dumper, data: Enum) -> yaml.Node:
            """Represent an Enum as a plain string in YAML"""
            return dumper.represent_scalar('tag:yaml.org,2002:str', str(data.value))

        # Create a custom Dumper class
        class CustomDumper(Dumper):
            pass

        # Register the custom representer with the custom Dumper
        CustomDumper.add_representer(Enum, enum_representer)

        # We don't save FAIR records for system datastreams
        if self.dp_tree.sensor.config.sensor_type == api.SENSOR_TYPE.SYS:
            return

        sensor_type = self.dp_tree.sensor.config.sensor_type

        # Wrap the "record" data in a FAIR record
        wrap: dict[str, dict | str | list] = {}
        wrap[api.RECORD_ID.VERSION.value] = "V3"
        wrap[api.RECORD_ID.DATA_TYPE_ID.value] = sensor_type.value
        wrap[api.RECORD_ID.DEVICE_ID.value] = self.device_id
        wrap[api.RECORD_ID.SENSOR_INDEX.value] = str(self.sensor_index)
        wrap[api.RECORD_ID.TIMESTAMP.value] = api.utc_to_iso_str()

        # Dump all the config from the DPtree
        wrap["sensor_config"] = self.dp_tree.export()

        # Dump the device config
        wrap["device_config"] = asdict(root_cfg.my_device)

        # Add system config
        if root_cfg.system_cfg is not None:
            wrap["system_config"] = root_cfg.system_cfg.model_dump()

        # Code version info
        expidite_version, user_code_version = root_cfg.get_version_info()
        wrap["expidite_version"] = expidite_version
        wrap["user_code_version"] = user_code_version

        # Storage account name
        if root_cfg.keys is not None:
            wrap["storage_account"] = Keys.get_storage_account(root_cfg.keys)

        # We always include the list of mac addresses for all devices in this experiment (fleet_config)
        # This enables the dashboard to check that all devices are present and working.
        fleet_macs = root_cfg.INVENTORY.keys()
        fleet_names = [root_cfg.INVENTORY[mac].name for mac in fleet_macs]
        fleet_dict = {mac: name for mac, name in zip(fleet_macs, fleet_names)}
        wrap["fleet"] = fleet_dict

        # Save the FAIR record as a YAML file to the FAIR archive
        fair_fname = file_naming.get_FAIR_filename(sensor_type, self.sensor_index, suffix="yaml")
        Path(fair_fname).parent.mkdir(parents=True, exist_ok=True)
        with open(fair_fname, "w") as f:
            yaml.dump(wrap, f, Dumper=CustomDumper)
        self.cc.upload_to_container(root_cfg.my_device.cc_for_fair, 
                                    [fair_fname], 
                                    delete_src=True,
                                    storage_tier=api.StorageTier.COOL)

    def log_sample_data(self, sample_period_start_time: datetime) -> None:
        """Provide the count & duration of data samples recorded (environmental, media, etc)
        since the last time log_sample_data was called.

        This is used by EdgeOrchestrator to periodically log observability data
        """
        # We need to traverse all nodes in the tree and call log_sample_data on each node
        for node in self.dp_tree._nodes.values():
            node.log_sample_data(sample_period_start_time)

    def get_sensor_cfg(self) -> Optional[SensorCfg]:
        """Return the SensorCfg object for this Datastream"""
        return self.dp_tree.sensor.config


    #########################################################################################################
    #
    # DPworker worker thread methods
    #
    #########################################################################################################

    def start(self) -> None:
        """Start the Datastream worker thread.

        In EDGE mode this is called by the Sensor class when the Sensor is started.
        In ETL mode this is called by DatastreamFactory when the ETL process is scheduled.
        """
        if self.dpe_start_time is None:
            self.dpe_start_time = api.utc_now()
            # Call our superclass Thread start() method which schedule our run() method
            logger.info(f"Starting DPworker {self} in {root_cfg.get_mode()} mode")
            super().start()
        else:
            logger.warning(f"{root_cfg.RAISE_WARN()}Datastream {self} already started.")

    def stop(self) -> None:
        """Stop the Datastream worker thread"""

        self._stop_requested.set()
        logger.info(f"Stopping DPworker {self}")

    def run(self) -> None:
        """Main DataProcessor thread that persistently processes files, logs or data generated by Sensors"""

        try:
            logger.info(f"Invoking run() on {self!r} in {root_cfg.get_mode()} mode")
            if root_cfg.get_mode() == Mode.EDGE:
                self.edge_run()
            else:
                assert False, "ETL mode not implemented yet"
        except Exception as e:
            logger.error(f"{root_cfg.RAISE_WARN()}Fatal error running {self!r}: {e!s}", 
                         exc_info=True)
            # @@@ Should we add recovery code? eg call stop_all?

    def edge_run(self) -> None:
        """Main Datastream loop processing files, logs or data generated by Sensors"""

        # Create the FAIR record for this sensor and associated processing
        self.save_FAIR_record()

        # If there are no data processors, we can exit the thread because data will be saved 
        # directly to the cloud
        if len(self.dp_tree.get_processors()) == 0:
            logger.debug(f"No DataProcessors registered; exiting DPworker loop; {self!r}")
            return

        while not self._stop_requested.is_set():
            start_time = api.utc_now()

            for edge in self.dp_tree.get_edges():
                try:
                    exec_start_time = api.utc_now()
                    assert isinstance(edge.sink, DataProcessor)
                    dp: DataProcessor = edge.sink
                    stream = edge.stream

                    #########################################################################################
                    # Invoke the DataProcessor
                    #
                    # Standard chaining involves passing a Dataframe along the DP chain.
                    # The first DP may be invoked with recording files (jpg, h264, wav, etc) or a CSV
                    # as defined in the dp_config
                    #########################################################################################
                    if stream.format in api.DATA_FORMATS:
                        # Find and load CSVs as DFs
                        input_df = self._get_csv_as_df(stream)
                        if input_df is not None:
                            logger.debug(f"Invoking {dp} with {input_df}")
                            dp.process_data(input_df)
                    else:
                        # DPs may process recording files
                        input_files = self._get_stream_files(stream)
                        if input_files is not None and len(input_files) > 0:
                            logger.debug(f"Invoking {dp} with {len(input_files)} files")
                            dp.process_data(input_files)

                            # Clear up the files now they've been processed.
                            # Any files that were meant to be uploaded will have been moved directly
                            # to the upload directory.
                            # Sampling is done on the initial save_recording.
                            for f in input_files:
                                if f.exists():
                                    try:
                                        f.unlink()
                                    except Exception as e:
                                        logger.error(f"{root_cfg.RAISE_WARN()}Failed to unlink {f} {e!s}", 
                                                     exc_info=True)
                                else:
                                    logger.error(f"{root_cfg.RAISE_WARN()}File does not exist after DP {f}")

                    # Log the processing time
                    exec_time = api.utc_now() - exec_start_time
                    dp._scorp_stat(stream.index, duration=exec_time.total_seconds())
                except Exception as e:
                    logger.error(
                        f"{root_cfg.RAISE_WARN()}Error processing files for {self}. e={e!s}",
                        exc_info=True,
                    )

            # We want to run this loop every minute, so see how long it took us since the start_time
            sleep_time = root_cfg.DP_FREQUENCY - (api.utc_now() - start_time).total_seconds()
            logger.debug(f"DataProcessor ({dp}) sleeping for {sleep_time} seconds")
            if sleep_time > 0:
                self._stop_requested.wait(sleep_time)

    def _get_stream_files(self, stream: Stream) -> Optional[list[Path]]:
        """Find any files that match the requested Datastream (type, device_id & sensor_index)"""
        if root_cfg.get_mode() == Mode.EDGE:
            src = root_cfg.EDGE_PROCESSING_DIR
        else:
            src = root_cfg.ETL_PROCESSING_DIR
        data_id = stream.get_data_id(self.sensor_index)
        files = list(src.glob(f"*{data_id}*.{stream.format.value}"))

        # We must return only files that are not currently being written to
        # Do not return files modified in the last few seconds
        now = api.utc_now().timestamp()
        files = [f for f in files if (now - f.stat().st_mtime) > 5]

        logger.debug(f"_get_ds_files returning {len(files)} files for {data_id}")
        return files

    def _get_csv_as_df(self, stream: Stream) -> Optional[pd.DataFrame]:
        """Get the first CSV file that matches this Datastream's DatastreamType as a DataFrame"""
        if root_cfg.get_mode() == Mode.EDGE:
            src = root_cfg.EDGE_PROCESSING_DIR
        else:
            src = root_cfg.ETL_PROCESSING_DIR

        data_id = stream.get_data_id(self.sensor_index)
        csv_files = src.glob(f"*{data_id}*.csv")

        df_list = []
        for csv_file in csv_files:
            try:
                df_list.append(pd.read_csv(csv_file))
            except Exception as e:
                logger.error(f"{root_cfg.RAISE_WARN()}Error reading CSV file {csv_file}: {e}", exc_info=True)
        
        # Concat all DataFrames into one
        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            logger.debug(f"Loaded {len(df)} rows from CSV files for {data_id}")
        else:
            logger.debug(f"No CSV files found for {data_id}")
        return df
