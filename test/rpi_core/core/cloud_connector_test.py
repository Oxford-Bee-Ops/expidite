import logging
from time import sleep

import pandas as pd

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import AsyncCloudConnector, CloudConnector, LocalCloudConnector

logger = root_cfg.setup_logger("expidite", level=logging.DEBUG)

DST_CONTAINER = "expidite-upload"


class TestCloudConnector:
    """CloudConnector defines the following methods:

    def get_instance(type: Optional[CloudType]=CloudType.AZURE) -> "CloudConnector":
    def stop(self) -> None:
    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: Optional[bool] = True,
        blob_tier: Enum=BlobTier.HOT,
    def download_from_container(
        self, src_container: str, src_file: str, dst_file: Path
    def download_container(
        self,
        src_container: str,
        dst_dir: Path,
        folder_prefix_len: Optional[int] = None,
        files: Optional[list[str]] = None,
        overwrite: bool = True,
    def move_between_containers(
        self,
        src_container: str,
        dst_container: str,
        blob_names: list[str],
        delete_src: bool = False,
        blob_tier: Enum =BlobTier.COOL,
    def append_to_cloud(
        self, dst_container: str, src_file: Path
    def container_exists(self, container: str) -> bool:
    def exists(self, src_container: str, blob_name: str) -> bool:
    def delete(self, container: str, blob_name: str) -> None:
    def list_cloud_files(
        self,
        container: str,
        prefix: Optional[str] = None,
        suffix: Optional[str] = None,
        more_recent_than: Optional[datetime] = None,
    def get_blob_modified_time(self, container: str, blob_name: str) -> datetime:
    """  # noqa: D415

    def test_production_cloud_connector(self) -> None:
        """Test the AsyncCloudConnector."""
        logger.info("Testing AsyncCloudConnector")
        # Get instance uses the test mode to decide which subclass to return
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        root_cfg.ST_MODE = root_cfg.SOFTWARE_TEST_MODE.TESTING  # Reset to test mode
        assert cc is not None, "CloudConnector instance is None"
        assert isinstance(cc, AsyncCloudConnector)

        # Run the standard set of tests for the CloudConnector
        self.set_of_cc_tests(cc)

        # Run additional tests for function only implemented in the production CloudConnector.
        self._test_zero_byte_file_in_cloud(cc)
        self._test_mismatched_headers_in_cloud(cc)

        cc.shutdown()

    def test_local_cloud_connector(self) -> None:
        """Test the LocalCloudConnector."""
        logger.info("Testing LocalCloudConnector")
        # Get instance uses the test mode to decide which subclass to return
        cc = CloudConnector.get_instance(root_cfg.CloudType.LOCAL_EMULATOR)
        assert cc is not None, "CloudConnector instance is None"
        assert isinstance(cc, LocalCloudConnector)

        # Run the standard set of tests for the CloudConnector
        self.set_of_cc_tests(cc)

        cc.shutdown()

    def set_of_cc_tests(self, cc: CloudConnector) -> None:
        """Standard set of actions that should work on any type of CloudConnector."""
        logger.info("Running standard set of CloudConnector tests")
        # Test upload with a dummy file and container name
        # Create a temporary file for testing
        src_file = file_naming.get_temporary_filename(api.FORMAT.TXT)
        with open(src_file, "w") as f:
            f.write("This is a test file.")
        cc.upload_to_container(DST_CONTAINER, [src_file], delete_src=False)

        # Upload is asynchronous, so we need to wait for it to complete
        sleep(1)

        # List files in the container to verify upload
        files = cc.list_cloud_files(DST_CONTAINER)
        logger.debug(f"Files in container {DST_CONTAINER}: {len(files)}")
        assert len(files) > 0, "No files found in cloud container after upload"

        # Test exists()
        assert cc.exists(DST_CONTAINER, src_file.name), "File does not exist in cloud container"

        # Test container_exists()
        assert cc.container_exists(DST_CONTAINER), "Container does not exist in cloud"

        # Test download_from_container()
        dst_file = file_naming.get_temporary_filename(api.FORMAT.TXT)
        cc.download_from_container(DST_CONTAINER, src_file.name, dst_file)
        assert dst_file.exists(), "Downloaded file does not exist"

        # Test append_to_cloud()
        # Create a temporary CSV file for appending
        append_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df.to_csv(append_file, index=False)
        assert append_file.exists(), "Append file does not exist"
        cc.append_to_cloud(DST_CONTAINER, append_file, delete_src=False)
        sleep(1)
        assert cc.exists(DST_CONTAINER, append_file.name), "Appended file does not exist in cloud container"
        assert append_file.exists(), "Append file does not exist after append"

        # Test get_blob_modified_time
        modified_time = cc.get_blob_modified_time(DST_CONTAINER, append_file.name)
        assert modified_time is not None, "Modified time is None"

        # Append to the same file again
        cc.append_to_cloud(DST_CONTAINER, append_file, delete_src=True)
        sleep(1)
        assert cc.exists(DST_CONTAINER, append_file.name), "Appended file does not exist in cloud container"
        assert not append_file.exists(), "Append file exists after second append despre delete_src=True"

        # Check the modified time again
        modified_time2 = cc.get_blob_modified_time(DST_CONTAINER, append_file.name)
        assert modified_time2 is not None, "Modified time is None after second append"
        assert modified_time2 > modified_time, "Modified time did not change after second append"

        # Download the appended file to verify its contents
        downloaded_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
        cc.download_from_container(DST_CONTAINER, append_file.name, downloaded_file)
        assert downloaded_file.exists(), "Downloaded appended file does not exist"

        # Read the downloaded file and check its contents
        downloaded_df = pd.read_csv(downloaded_file)
        assert not downloaded_df.empty, "Downloaded appended file is empty"
        assert len(downloaded_df) == 4, f"Downloaded appended file has insufficient rows {len(downloaded_df)}"

        # Delete the test file in the cloud and check it is gone
        cc.delete(DST_CONTAINER, src_file.name)
        assert not cc.exists(DST_CONTAINER, src_file.name), "File still exists after delete"

    def _test_zero_byte_file_in_cloud(self, cc: CloudConnector) -> None:
        # Test append_to_cloud() with a zero-byte file already in cloud storage
        zero_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
        zero_file.touch()
        cc.upload_to_container(DST_CONTAINER, [zero_file], delete_src=True)
        sleep(1)
        assert cc.exists(DST_CONTAINER, zero_file.name), "Zero-byte file does not exist in cloud"

        # Write CSV data to the same local path and append to the zero-byte cloud blob
        df_zero = pd.DataFrame({"col1": [10, 20], "col2": [30, 40]})
        df_zero.to_csv(zero_file, index=False)
        cc.append_to_cloud(DST_CONTAINER, zero_file, delete_src=True)
        sleep(1)
        assert cc.exists(DST_CONTAINER, zero_file.name), "File does not exist after append to zero-byte blob"

        # Download and verify the result has headers and data
        dl_zero = file_naming.get_temporary_filename(api.FORMAT.CSV)
        cc.download_from_container(DST_CONTAINER, zero_file.name, dl_zero)
        assert dl_zero.exists(), "Downloaded zero-byte append file does not exist"
        dl_zero_df = pd.read_csv(dl_zero)

        assert not dl_zero_df.empty, "Appended data to zero-byte blob is empty"
        assert len(dl_zero_df) == 2, f"Expected 2 rows after append to zero-byte blob, got {len(dl_zero_df)}"
        assert list(dl_zero_df.columns) == ["col1", "col2"], "Headers missing after append to zero-byte blob"

        cc.delete(DST_CONTAINER, zero_file.name)

        sleep(1)

    def _test_mismatched_headers_in_cloud(self, cc: CloudConnector) -> None:
        # Test append_to_cloud() when the cloud file has different headers than the local file.
        mismatch_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
        df_original = pd.DataFrame({"col_a": [1, 2], "col_b": [3, 4]})
        df_original.to_csv(mismatch_file, index=False)
        cc.append_to_cloud(DST_CONTAINER, mismatch_file, delete_src=False)
        sleep(1)
        assert cc.exists(DST_CONTAINER, mismatch_file.name), "Original file does not exist in cloud"

        # Append data with different headers to the same cloud blob. Need to force a new instance of
        # CloudConnector to simulate restarting after an upgrade.
        cc.shutdown()
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)

        df_new = pd.DataFrame({"col_b": [5, 6], "col_c": [7, 8]})
        df_new.to_csv(mismatch_file, index=False)
        cc.append_to_cloud(DST_CONTAINER, mismatch_file, delete_src=True)
        sleep(1)
        assert cc.exists(DST_CONTAINER, mismatch_file.name), "File does not exist after mismatched append"

        # Download and verify the result has merged headers and all data.
        dl_mismatch = file_naming.get_temporary_filename(api.FORMAT.CSV)
        cc.download_from_container(DST_CONTAINER, mismatch_file.name, dl_mismatch)
        assert dl_mismatch.exists(), "Downloaded mismatched-header file does not exist"
        dl_mismatch_df = pd.read_csv(dl_mismatch)

        assert not dl_mismatch_df.empty, "Merged data is empty"
        assert len(dl_mismatch_df) == 4, (
            f"Expected 4 rows after mismatched-header append, got {len(dl_mismatch_df)}"
        )
        assert "col_a" in dl_mismatch_df.columns, "col_a missing from merged headers"
        assert "col_b" in dl_mismatch_df.columns, "col_b missing from merged headers"
        assert "col_c" in dl_mismatch_df.columns, "col_c missing from merged headers"

        cc.delete(DST_CONTAINER, mismatch_file.name)

        cc.shutdown()
        sleep(1)
