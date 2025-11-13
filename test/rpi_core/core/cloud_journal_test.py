
from time import sleep

import pytest

from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import AsyncCloudConnector, CloudConnector
from expidite_rpi.utils.cloud_journal import CloudJournal
from expidite_rpi.utils.journal import Journal

logger = root_cfg.setup_logger("expidite")
root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING


####################################################################################################
# Test CloudJournal & Journal
#
# The CloudJournal is a Journal that automatically uploads to the cloud.
####################################################################################################
class Test_CloudJournal:
    @pytest.mark.unittest
    def test_CloudJournal(self) -> None:
        logger.info("Run test_CloudJournal test")

        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        assert isinstance(cc, AsyncCloudConnector)

        # Create test data
        reqd_columns = ["field1", "field2", "field3"]
        test_data = {"field1": 1, "field2": 2, "field3": 3}
        test_journal_path = root_cfg.TMP_DIR.joinpath("test.csv")

        # Delete any old files locally and in the cloud
        if test_journal_path.exists():
            test_journal_path.unlink()
        if cc.exists(root_cfg.my_device.cc_for_upload, test_journal_path.name):
            cc.delete(root_cfg.my_device.cc_for_upload, test_journal_path.name)

        test_journal = Journal(test_journal_path, reqd_columns=reqd_columns)

        test_journal.add_row(test_data)
        test_journal.save()

        # Create the CloudJournal and add the test data
        cj = CloudJournal(
            test_journal_path,
            root_cfg.my_device.cc_for_upload,
            reqd_columns=reqd_columns,
        )
        # This is asynchronous, so we need to wait for the worker thread to start
        sleep(1)

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
        sleep(2)

        # Download the file again to check the data integrity
        # This will fail if pandas can't parse the file
        cj.download()

        ###########################################################################
        # Repeat after having changed the reqd_columns
        # We'll only ever encounter this when we change the coded definition
        ###########################################################################
        # Because CC only checks for mismatched columns when it is first writing to a new file,
        # we need to delete it's cache of known files
        cc._validated_append_files = set()

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
        sleep(2)

        # Download the file again to check the data integrity
        # This will fail if pandas can't parse the file
        cj.download()

        # Stop the worker thread so we exit
        cj.manager.stop()
        # Stop the cloudconnector
        cc.shutdown()
