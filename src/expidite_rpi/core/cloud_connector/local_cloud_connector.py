import shutil
from datetime import UTC, datetime
from pathlib import Path

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector.cloud_connector import CloudConnector

logger = root_cfg.setup_logger("expidite")


##############################################################################################################
# LocalCloudConnector class
#
# This class is used to connect to the local cloud emulator. It is a subclass of CloudConnector and implements
# the same interface. It is used for testing purposes only and should not be used in production.
##############################################################################################################
class LocalCloudConnector(CloudConnector):
    def __init__(self) -> None:
        logger.debug("Creating LocalCloudConnector instance")
        super().__init__()

        if not root_cfg.system_cfg.is_valid:
            msg = "System configuration not set; cannot connect to cloud"
            raise ValueError(msg)

        self.local_cloud = (
            root_cfg.ROOT_WORKING_DIR
            / root_cfg.system_cfg.local_cloud
            / root_cfg.my_device_id
            / api.utc_to_fname_str()
        )

    def get_local_cloud(self) -> Path:
        """Creates a local cloud directory. Usually called by RpiEmulator.__enter__() as
        when the RpiEmulator is used as a context manager.

        This is an unpredictable string so we don't clash with other local cloud instances.
        """
        # shutil.rmtree(self.local_cloud)
        if not self.local_cloud.exists():
            self.local_cloud.mkdir(parents=True, exist_ok=True)
        return self.local_cloud

    def clear_local_cloud(self) -> None:
        """Clear the local cloud storage - this is used for testing only."""
        if self.local_cloud.exists():
            shutil.rmtree(self.local_cloud)

    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: bool,
        storage_tier: api.StorageTier = api.StorageTier.HOT,
    ) -> None:
        """Upload a list of local files to an Azure container
        These will be regular block_blobs and not append_blobs; see append_to_cloud for append_blobs.

        Parameters:
            dst_container: destination ContainerClient
            src_files: list of files to upload to the container
            delete_src: delete the local src_files instances after successful upload; defaults to True

        If the upload fails part way through, those files that were successfully uploaded will have
        been deleted (if delete_src=True), while any remaining files in src_files will not have been
        deleted.
        """
        for file in src_files:
            if file.exists():
                # Copy the file to the local cloud directory
                dst_file = self.local_cloud / dst_container / file.name
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                if delete_src:
                    shutil.move(file, dst_file)
                else:
                    shutil.copy(file, dst_file)

    def download_from_container(self, src_container: str, src_file: str, dst_file: Path) -> None:
        """Downloads the src_file to a local dst_file Path."""
        if not dst_file.parent.exists():
            dst_file.parent.mkdir(parents=True, exist_ok=True)

        if dst_file.exists():
            dst_file.unlink()

        shutil.copy(self.local_cloud / src_container / src_file, dst_file)

    def download_container(
        self,
        src_container: str,
        dst_dir: Path,
        folder_prefix_len: int | None = None,
        files: list[str] | None = None,
        overwrite: bool = True,
    ) -> None:
        """Download all the files in the src_container to the dst_dir.

        Parameters:
            src_container: source container
            dst_dir: destination directory to download the files to
            folder_prefix_len: Optional; first n characters of the file name to use as a subfolder
            files: Optional; list of files to download from src_container; if None, all files in the container
                will be downloaded; useful for chunking downloads
            overwrite: If False, function will skip downloading files that already exist in dst_dir
        """
        download_container = self.local_cloud / src_container

        if files is None:
            for blob in download_container.glob("*"):
                if folder_prefix_len is not None:
                    folder_dir = dst_dir / blob.name[:folder_prefix_len]
                else:
                    folder_dir = dst_dir
                if not folder_dir.exists():
                    folder_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(blob, folder_dir)
        else:
            for blob_name in files:
                src_file = download_container / blob_name
                if folder_prefix_len is not None:
                    folder_dir = dst_dir / blob_name[:folder_prefix_len]
                else:
                    folder_dir = dst_dir
                dst_file = folder_dir / blob_name
                if not overwrite and dst_file.exists():
                    logger.debug(f"File {dst_file} already exists; skipping download")
                    continue
                shutil.copy(src_file, dst_file)

    def move_between_containers(
        self,
        src_container: str,
        dst_container: str,
        blob_names: list[str],
        delete_src: bool = False,
        storage_tier: api.StorageTier = api.StorageTier.COOL,
    ) -> None:
        """Move blobs between containers.

        Parameters:
            src_container: source container
            dst_container: destination container
            blob_names: list of blob names to move
            delete_src: delete the source blobs after successful upload; defaults to False
        """
        for blob_name in blob_names:
            shutil.copy(
                self.local_cloud / src_container / blob_name, self.local_cloud / dst_container / blob_name
            )
            if delete_src:
                (self.local_cloud / src_container / blob_name).unlink()

            logger.debug(
                f"Moved {blob_name} from {src_container} to {dst_container}"
                f" and {'deleted' if delete_src else 'did not delete'} the source"
            )

    def append_to_cloud(
        self, dst_container: str, src_file: Path, delete_src: bool, col_order: list[str] | None = None
    ) -> bool:
        """Append a block of CSV data to an existing CSV file in the cloud.

        Parameters:
            dst_container: destination container
            src_file: source file Path
            delete_src: delete the local src_file instance after successful upload

        Returns:
            bool indicating whether data was successfully written to the blob

        If the remote file doesn't already exist it will be created.
        If the remote file does exist, the first line (headers) in the src_file will be dropped
        so that we don't duplicate a header row.
        It the responsibility of the calling function to ensure that the columns & headers in the
        CSV data are consistent between local and remote files
        """
        blob_client = self.local_cloud / dst_container / src_file.name
        try:
            logger.debug(f"LocalCC.append_to_cloud() with delete_src={delete_src} for {src_file}")

            # Read the local file data ready to append
            with src_file.open("r") as file:
                local_lines = file.readlines()
                if len(local_lines) == 1:
                    return False  # No data beyond headers

            if not blob_client.exists():
                # Include the Headers
                data_to_append = "".join(local_lines[:])
                # Create the file
                blob_client.parent.mkdir(parents=True, exist_ok=True)
                blob_client.touch()
            else:
                # Drop the Headers in the first line so we don't have repeat header rows.
                data_to_append = "".join(local_lines[1:])

            # Append the data to the local file
            with blob_client.open("a") as blob_file:
                blob_file.write(data_to_append)

            if delete_src:
                logger.debug(f"Deleting append file: {src_file}")
                src_file.unlink()

            return True
        except Exception:
            logger.exception(f"{root_cfg.RAISE_WARN()}Failed to append data to {blob_client}")
            return False

    def container_exists(self, container: str) -> bool:
        """Check if the specified container exists."""
        # We always return true in the emulator; creating the container if it doesn't exist
        container_client = self.local_cloud / container
        if not container_client.exists():
            container_client.mkdir(parents=True, exist_ok=True)
        return True

    def create_container(self, container: str) -> None:
        """Create the specified container."""
        container_client = self.local_cloud / container
        if not container_client.exists():
            logger.info(f"Creating container {container}")
            container_client.mkdir(parents=True, exist_ok=True)

    def exists(self, src_container: str, blob_name: str) -> bool:
        """Check if the specified blob exits."""
        blob_client = self.local_cloud / src_container / blob_name
        return blob_client.exists()

    def delete(self, container: str, blob_name: str) -> None:
        """Delete specified blob."""
        blob_client = self.local_cloud / container / blob_name
        blob_client.unlink()

    def list_cloud_files(
        self,
        container: str,
        prefix: str | None = None,
        suffix: str | None = None,
        more_recent_than: datetime | None = None,
    ) -> list[str]:
        """Similar to the Path.glob() method but against a cloud container.

        Parameters:
            - container: container to be searched
            - prefix: prefix to match to files in the container; does not support wildcards
            - suffix: suffix to match to files in the container
            - more_recent_than: Optional; if specified, only files more recent than this date will be returned

        The current backend implementation is the Azure Blobstore which only supports prefix search
        and tag search.
        """
        container_client = self.local_cloud / container

        query = f"{prefix}*" if prefix is not None else "*"
        file_paths = list(container_client.glob(query))
        files = [f.name for f in file_paths]

        if suffix is not None:
            files = [f for f in files if f.endswith(suffix)]

        if more_recent_than is not None:
            files = [f for f in files if file_naming.get_file_datetime(f) > more_recent_than]
        logger.debug(f"list_cloud_files returning {len(files)!s} files")

        return files

    def get_blob_modified_time(self, container: str, blob_name: str) -> datetime:
        """Get the last modified time of the specified blob."""
        container_client = self.local_cloud / container
        blob_client = container_client / blob_name
        if blob_client.exists():
            last_modified = blob_client.stat().st_mtime
            # The Azure timezone is UTC but it's not explicitly set; set it
            return datetime.fromtimestamp(last_modified, tz=UTC)
        logger.warning(f"Blob {blob_name} does not exist in container {container}")
        return datetime.min.replace(tzinfo=UTC)
