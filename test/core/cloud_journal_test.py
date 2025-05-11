
from time import sleep

import pytest
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import AsyncCloudConnector, CloudConnector
from sensor_core.utils.cloud_journal import CloudJournal
from sensor_core.utils.journal import Journal

logger = root_cfg.setup_logger("sensor_core")
root_cfg.TEST_MODE = root_cfg.MODE.TEST


####################################################################################################
# Test CloudJournal & Journal
#
# The CloudJournal is a Journal that automatically uploads to the cloud.
####################################################################################################
class Test_CloudJournal:
    @pytest.mark.quick
    def test_CloudJournal(self) -> None:

        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        assert isinstance(cc, AsyncCloudConnector)

        # Create test data
        reqd_columns = ["field1", "field2", "field3"]
        test_data = {"field1": 1, "field2": 2, "field3": 3}
        test_journal_path = root_cfg.TMP_DIR.joinpath("test.csv")
        if test_journal_path.exists():
            test_journal_path.unlink()
        test_journal = Journal(test_journal_path, reqd_columns=reqd_columns)

        test_journal.add_row(test_data)
        test_journal.save()

        if cc.exists(root_cfg.my_device.cc_for_upload, test_journal_path.name):
            cc.delete(root_cfg.my_device.cc_for_upload, test_journal_path.name)

        # Create the CloudJournal and add the test data
        cj = CloudJournal(
            test_journal_path,
            root_cfg.my_device.cc_for_upload,
            reqd_columns=reqd_columns,
        )

        cj.add_rows_from_df(test_journal.as_df())
        cj.flush_all()
        sleep(1)

        # Check that the file exists in the cloud
        cj.download()
        cj.add_rows(test_journal.get_data())
        cj.flush_all()

        # Test a mismatch between local and existing columns
        # This will succeed because field4 will be dropped because it's not in the reqd_columns
        test_data["field4"] = 4
        cj.add_row(test_data)
        cj.flush_all()

        # Repeat after having changed the reqd_columns
        # We'll only ever encounter this when we change the coded definition
        reqd_columns = ["field1", "field2", "field3", "field4"]
        test_data = {"field1": 1, "field2": 2, "field3": 3, "field4": 4}
        test_journal = Journal(test_journal_path, reqd_columns=reqd_columns)
        cj = CloudJournal(
            test_journal_path,
            root_cfg.my_device.cc_for_upload,
            reqd_columns=reqd_columns,
        )
        cj.add_row(test_data)
        cj.flush_all()

        # Stop the worker thread so we exit
        cj.manager.stop()
        # Stop the cloudconnector
        cc.shutdown()
