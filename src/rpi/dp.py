from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from sensor_core import configuration as root_cfg
from sensor_core.dp_config_objects import DataProcessorCfg
from sensor_core.dp_node import DPnode

logger = root_cfg.setup_logger("sensor_core")


####################################################################################################
#
# Class: DataProcessor
#
#####################################################################################################
class DataProcessor(DPnode, ABC):
    """DataProcessors are invoked by the Datastream to process data from a Sensor.

    The DataProcessor implements the process_data() function to process the Sensor data.
    DataProcessors are commonly chained together to process data in sequence.
    The chain is defined in the Datastream configuration in configuration.py as a list of
    DataProcessorConfig objects.

    DataProcessors can define 'derived' Datastreams to enable forking of the data pipeline by
    implementing the define_derived_datastreams.
    """
    def __init__(self, 
                 config: DataProcessorCfg,
                 sensor_index: int, 
    ) -> None:
        DPnode.__init__(self, config, sensor_index)
        self.config = config


    @abstractmethod
    def process_data(
        self, 
        input_data: pd.DataFrame | list[Path],
    ) -> None:
        """Subclasses of this method provide custom processing of sensor data.

        DPs may be invoked with either a DataFrame or a list of files.
        DPs can save output by calling:
        - self.log()
        - self.save_data()
            - Every row in the DataFrame must contain the api.REQD_RECORD_ID_FIELDS.
        - self.save_recording()
            - To save a recording as a file (eg an image).
        - self.save_sub_recording()
            - To save a sub-recording as a file (eg a sub sample of a video).

        All DataProcessors must subclass this method.
        """

        assert False, "DataProcessor subclass must implement process_data()"
