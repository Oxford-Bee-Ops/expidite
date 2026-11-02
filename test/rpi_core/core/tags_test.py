"""Device tags are recorded in the FAIR record only, never in the data journals."""

from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import SensorCfg, Stream
from expidite_rpi.core.edge_orchestrator import EdgeOrchestrator
from expidite_rpi.core.sensor import Sensor
from expidite_rpi.example.my_fleet_config import INVENTORY
from expidite_rpi.rpi_core import RpiCore
from expidite_rpi.utils import rpi_emulator
from expidite_rpi.utils.journal_pool import CloudJournalPool

logger = root_cfg.setup_logger("expidite")

TAGS = {"Customer": "Koppert", "Serial number": "2"}
TAGS_COL = "tags"
TEST_DEVICE_ID = "d01111111111"

STREAM = Stream(
    description="Stream used to exercise tags",
    type_id="TESTTAGS",
    index=0,
    format=api.FORMAT.DF,
    fields=["value"],
)


class _TestSensor(Sensor):
    __test__ = False  # Stop pytest collecting this as a test class.


def _make_sensor() -> _TestSensor:
    return _TestSensor(
        SensorCfg(
            outputs=[STREAM],
            description="Sensor used to exercise tags",
            sensor_type=api.SENSOR_TYPE.I2C,
            sensor_index=0,
            sensor_model="TestSensor",
        )
    )


class Test_journals_exclude_tags:
    @pytest.mark.unittest
    def test_save_data_output_has_no_tags_column(self) -> None:
        sensor = _make_sensor()
        with patch.object(root_cfg, "my_device", replace(root_cfg.my_device, tags=TAGS)):
            output = sensor._validate_output(pd.DataFrame({"value": [1, 2, 3]}), STREAM)
        assert TAGS_COL not in output.columns

    @pytest.mark.unittest
    def test_log_output_has_no_tags_field(self) -> None:
        sensor = _make_sensor()
        cpool = MagicMock()
        with (
            patch.object(root_cfg, "my_device", replace(root_cfg.my_device, tags=TAGS)),
            patch.object(sensor, "_get_cpool", return_value=cpool),
        ):
            sensor.log(STREAM.index, {"value": 1})
        rows: list[dict[str, object]] = cpool.add_rows.call_args.args[1]
        assert len(rows) == 1
        assert TAGS_COL not in rows[0]

    @pytest.mark.unittest
    def test_journal_columns_exclude_tags(self) -> None:
        """The journal's column list decides what reaches the CSV."""
        assert TAGS_COL not in api.ALL_RECORD_ID_FIELDS
        with patch("expidite_rpi.utils.journal_pool.CloudJournal") as cloud_journal:
            CloudJournalPool()._get_journal(STREAM, datetime(2026, 11, 2, tzinfo=UTC))
        reqd_columns: list[str] = cloud_journal.call_args.args[2]
        assert TAGS_COL not in reqd_columns


class Test_FAIR_record_includes_tags:
    @pytest.mark.unittest
    def test_tags_in_fair_records(self) -> None:
        """Tags are kept in both the FAIR archive and the latest FAIR record."""
        root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING
        root_cfg.update_my_device_id(TEST_DEVICE_ID)

        with rpi_emulator.RpiEmulator.get_instance() as th:
            inventory = [replace(d, tags=TAGS) if d.device_id == TEST_DEVICE_ID else d for d in INVENTORY]
            RpiCore().configure(inventory)
            # No load_config(): it would leave sensor threads in the orchestrator singleton for later tests,
            # and the device config (which holds the tags) doesn't depend on it.
            EdgeOrchestrator.get_instance().save_FAIR_record()

            assert th.local_cloud is not None
            for container in (root_cfg.my_device.cc_for_fair, root_cfg.my_device.cc_for_fair_latest):
                files = list((th.local_cloud / container).glob(f"V3_{TEST_DEVICE_ID}*.yaml"))
                assert files, f"No FAIR record in {container}"
                for f in files:
                    # The FAIR record is written with a non-safe Dumper, so needs the matching loader.
                    record = yaml.load(f.read_text(), Loader=yaml.UnsafeLoader)  # noqa: S506
                    assert record["device_config"]["tags"] == TAGS
