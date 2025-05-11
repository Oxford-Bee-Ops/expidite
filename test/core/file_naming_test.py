from datetime import datetime, timedelta

import pytest
from example import my_fleet_config
from example.my_sensor_example import EXAMPLE_FILE_DS_TYPE_ID, EXAMPLE_FILE_STREAM_INDEX, EXAMPLE_SENSOR_CFG
from sensor_core import api, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.dp_tree import DPtree

logger = root_cfg.setup_logger("sensor_core")
root_cfg.TEST_MODE = root_cfg.MODE.TEST

class Test_datastream:

    @pytest.mark.quick
    def test_file_naming(self) -> None:
        my_example_dptree: DPtree = my_fleet_config.create_example_device()[0]
        stream = my_example_dptree.sensor.get_stream(EXAMPLE_FILE_STREAM_INDEX)
        data_id = stream.get_data_id(EXAMPLE_SENSOR_CFG.sensor_index)
        output_format = stream.format
        fname = file_naming.get_record_filename(
            root_cfg.EDGE_PROCESSING_DIR,
            data_id=data_id,
            suffix=output_format,
            start_time=api.utc_now() - timedelta(hours=1),
            end_time=api.utc_now(),
        )
        print(fname)
        fields = file_naming.parse_record_filename(fname)
        assert fields[api.RECORD_ID.DATA_TYPE_ID.value] == EXAMPLE_FILE_DS_TYPE_ID
        assert fields[api.RECORD_ID.DEVICE_ID.value] == "d01111111111"
        assert fields[api.RECORD_ID.SENSOR_INDEX.value] == EXAMPLE_SENSOR_CFG.sensor_index
        assert isinstance(fields[api.RECORD_ID.TIMESTAMP.value], datetime)
        assert (
            isinstance(fields[api.RECORD_ID.END_TIME.value], datetime)
            or fields[api.RECORD_ID.END_TIME.value] is None
        )
        assert fields[api.RECORD_ID.SUFFIX.value] == output_format.value


    @pytest.mark.quick
    def test_id_parsing(self) -> None:
        device_id = "d01111111111"
        type_id = "test"
        sensor_id = 1
        stream_index = 3
        output: file_naming.DATA_ID = file_naming.parse_data_id(
            data_id=file_naming.create_data_id(
                device_id=device_id, sensor_index=sensor_id, type_id=type_id, stream_index=stream_index
            )
        )
        assert output.device_id == device_id
        assert output.type_id == type_id
        assert output.sensor_index == sensor_id
        assert output.stream_index == stream_index
