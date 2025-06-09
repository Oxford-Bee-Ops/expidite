from pathlib import Path

import pandas as pd

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp import DataProcessor
from expidite_rpi.core.dp_config_objects import DataProcessorCfg, Stream

logger = root_cfg.setup_logger("expidite")

EXAMPLE_DF_TYPE_ID = "DUMMD"
EXAMPLE_DF_STREAM_INDEX = 0
EXAMPLE_FILE_PROCESSOR_CFG = DataProcessorCfg(
    description="Example file processor for testing",
    outputs=[Stream(description="Example dataframe stream",
                    type_id=EXAMPLE_DF_TYPE_ID, 
                    index=EXAMPLE_DF_STREAM_INDEX, 
                    format=api.FORMAT.DF, 
                    fields=["pixel_count"]),
            ],
)

#############################################################################################################
# Define the DataProcessor for the ExampleSensor
#
# The DataProcessor is responsible for processing the data from the Datastream.
# It must implement the process_data() method.
#
# This data processor:
# - processes files into DataFrames (primary Datastream)
# - creates data that it records into a derived Datastream
#############################################################################################################
class ExampleProcessor(DataProcessor):
    def process_data(
        self, 
        input_data: pd.DataFrame | list[Path]
    ) -> None:
        """This implementation of the process_data method is used in testing:
        - so has an excess number of asserts!
        - demonstrates a file DP converting a file list to a DataFrame
        - demonstrates a DF DP returning a DataFrame"""
        assert input_data is not None
        assert isinstance(input_data, list)

        logger.debug(f"ExampleProcessor process_data:{input_data} for {__name__}")

        output_data: list[dict] = []    
        if len(input_data) > 0:
            for f in input_data:
                # Generate output to the primary datastream
                output_data.append({"pixel_count": 25})

        # Generate data for the derived datastream
        self.save_data(stream_index=EXAMPLE_DF_STREAM_INDEX,
                        sensor_data=pd.DataFrame(output_data))
                