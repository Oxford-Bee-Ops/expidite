import logging
from time import sleep

import pandas as pd
from sensor_core import api, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import AsyncCloudConnector, CloudConnector, LocalCloudConnector

logger = root_cfg.setup_logger("sensor_core", level=logging.DEBUG)

class TestCloudConnector:

    """ CloudConnector defines the following methods:

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
        overwrite: Optional[bool] = True,
    def move_between_containers(
        self,
        src_container: str,
        dst_container: str,
        blob_names: list[str],
        delete_src: bool = False,
        blob_tier: Enum =BlobTier.COOL,
    def append_to_cloud(
        self, dst_container: str, src_file: Path, safe_mode: Optional[bool] = False
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
    """
    def test_production_cloud_connector(self) -> None:
        """Test the AsyncCloudConnector."""
        logger.info("Testing AsyncCloudConnector")
        # Get instance uses the test mode to decide which subclass to return
        cc = CloudConnector.get_instance(root_cfg.CloudType.AZURE)
        root_cfg.TEST_MODE = root_cfg.MODE.TEST  # Reset to test mode
        assert cc is not None, "CloudConnector instance is None"
        assert isinstance(cc, AsyncCloudConnector)

        # Run the standard set of tests for the CloudConnector
        self.set_of_cc_tests(cc)
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


    def set_of_cc_tests(self, cc: CloudConnector) -> None:
        """Standard set of actions that should work on any type of CloudConnector."""
        # Test upload with a dummy file and container name
        # Create a temporary file for testing
        src_file = file_naming.get_temporary_filename(api.FORMAT.TXT)
        with open(src_file, "w") as f:
            f.write("This is a test file.")
        dst_container = "sensor-core-upload"
        cc.upload_to_container(dst_container, [src_file], delete_src=False)
        
        # Upload is asynchronous, so we need to wait for it to complete
        sleep(1)

        # List files in the container to verify upload
        files = cc.list_cloud_files(dst_container)
        logger.debug(f"Files in container {dst_container}: {len(files)}")
        assert len(files) > 0, "No files found in cloud container after upload"

        # Test exists()
        assert cc.exists(dst_container, src_file.name), "File does not exist in cloud container"

        # Test container_exists()
        assert cc.container_exists(dst_container), "Container does not exist in cloud"

        # Test download_from_container()
        dst_file = file_naming.get_temporary_filename(api.FORMAT.TXT)
        cc.download_from_container(dst_container, src_file.name, dst_file)
        assert dst_file.exists(), "Downloaded file does not exist"

        # Test append_to_cloud()
        # Create a temporary CSV file for appending
        append_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df.to_csv(append_file, index=False)
        assert append_file.exists(), "Append file does not exist"
        cc.append_to_cloud(dst_container, append_file, delete_src=False)
        sleep(1)
        assert cc.exists(dst_container, append_file.name), "Appended file does not exist in cloud container"
        assert append_file.exists(), "Append file does not exist after append"

        # Test get_blob_modified_time
        modified_time = cc.get_blob_modified_time(dst_container, append_file.name)
        assert modified_time is not None, "Modified time is None"

        # Append to the same file again
        cc.append_to_cloud(dst_container, append_file, delete_src=True, safe_mode=True)
        sleep(1)
        assert cc.exists(dst_container, append_file.name), "Appended file does not exist in cloud container"
        assert not append_file.exists(), "Append file exists after second append despre delete_src=True"

        # Check the modified time again
        modified_time2 = cc.get_blob_modified_time(dst_container, append_file.name)
        assert modified_time2 is not None, "Modified time is None after second append"
        assert modified_time2 > modified_time, "Modified time did not change after second append"

        # Download the appended file to verify its contents
        downloaded_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
        cc.download_from_container(dst_container, append_file.name, downloaded_file)    
        assert downloaded_file.exists(), "Downloaded appended file does not exist"

        # Read the downloaded file and check its contents
        downloaded_df = pd.read_csv(downloaded_file)
        assert not downloaded_df.empty, "Downloaded appended file is empty"
        assert len(downloaded_df) == 4, f"Downloaded appended file has insufficient rows {len(downloaded_df)}"

        # Delete the test file in the cloud and check it is gone
        cc.delete(dst_container, src_file.name)
        assert not cc.exists(dst_container, src_file.name), "File still exists after delete"


