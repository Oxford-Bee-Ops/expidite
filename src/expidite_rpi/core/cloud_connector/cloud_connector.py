import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from azure.storage.blob import BlobClient, ContainerClient

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.configuration import CloudType

logger = root_cfg.setup_logger("expidite")


##############################################################################################################
# Default implementation of the CloudConnector class and interface definition.
#
# This class is used to connect to the cloud storage provider (Azure Blob Storage) but does so synchronously.
##############################################################################################################
class CloudConnector:
    _instance: Optional["CloudConnector"] = None

    def __init__(self) -> None:
        if root_cfg.my_device is None:
            raise ValueError("System configuration not set; cannot connect to cloud")

        if root_cfg.keys is None or root_cfg.keys.cloud_storage_key == root_cfg.FAILED_TO_LOAD:
            raise ValueError("Cloud storage credentials not set; cannot connect to cloud")

        self._connection_string = root_cfg.keys.cloud_storage_key
        self._validated_containers: dict[str, ContainerClient] = {}
        self._validated_append_files: set[str] = set()

    @staticmethod
    def get_instance(cloud_type: CloudType) -> "CloudConnector":
        """We use a factory pattern to offer up alternative types of CloudConnector for accessing different
        cloud storage providers and / or the local emulator.
        """
        match cloud_type:
            case CloudType.AZURE:
                from expidite_rpi.core.cloud_connector.async_cloud_connector import AsyncCloudConnector

                if CloudConnector._instance is None:
                    CloudConnector._instance = AsyncCloudConnector()
                elif not isinstance(CloudConnector._instance, AsyncCloudConnector):
                    logger.warning(
                        f"{root_cfg.RAISE_WARN()}Replacing CloudConnector instance with AsyncCloudConnector"
                    )
                    CloudConnector._instance.shutdown()
                    CloudConnector._instance = AsyncCloudConnector()
                return CloudConnector._instance

            case CloudType.LOCAL_EMULATOR:
                from expidite_rpi.core.cloud_connector.local_cloud_connector import LocalCloudConnector

                if CloudConnector._instance is None:
                    CloudConnector._instance = LocalCloudConnector()
                elif not isinstance(CloudConnector._instance, LocalCloudConnector):
                    logger.warning(
                        f"{root_cfg.RAISE_WARN()}Replacing CloudConnector instance with LocalCloudConnector"
                    )
                    CloudConnector._instance.shutdown()
                    CloudConnector._instance = LocalCloudConnector()
                return CloudConnector._instance

            case CloudType.SYNC_AZURE:
                from expidite_rpi.core.cloud_connector.sync_cloud_connector import SyncCloudConnector

                if CloudConnector._instance is None:
                    CloudConnector._instance = SyncCloudConnector()
                elif not isinstance(CloudConnector._instance, SyncCloudConnector):
                    logger.warning(
                        f"{root_cfg.RAISE_WARN()}Replacing CloudConnector instance with SyncCloudConnector"
                    )
                    CloudConnector._instance.shutdown()
                    CloudConnector._instance = SyncCloudConnector()
                return CloudConnector._instance

    def set_keys(self, keys_file: Path | None = None, key: str | None = None) -> None:
        """Sets the cloud storage key for the CloudConnector from either a file or directly from a string."""
        if keys_file:
            if not keys_file.exists():
                msg = f"Keys file {keys_file} does not exist"
                raise ValueError(msg)

            # Create a new Keys class with the env_file set in the model_config
            keys = root_cfg.Keys(_env_file=keys_file, _env_file_encoding="utf-8")  # type: ignore
            if keys.cloud_storage_key == root_cfg.FAILED_TO_LOAD:
                msg = f"Failed to load cloud storage key from {keys_file}"
                raise ValueError(msg)

            self._connection_string = keys.cloud_storage_key
        else:
            assert key is not None
            self._connection_string = key

    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: bool,
        storage_tier: api.StorageTier = api.StorageTier.HOT,
    ) -> None:
        """Upload a list of local files to an Azure container.

        These will be regular block_blobs and not append_blobs; see append_to_cloud for append_blobs.

        Parameters:
            dst_container: destination ContainerClient
            src_files: list of files to upload to the container
            delete_src: delete the local src_files instances after successful upload; defaults to True

        If the upload fails part way through, those files that were successfully uploaded will have been
        deleted (if delete_src=True), while any remaining files in src_files will not have been deleted.
        """
        upload_container = self._validate_container(dst_container)

        for file in src_files:
            if file.exists():
                blob_client = upload_container.get_blob_client(file.name)
                with open(file, "rb") as data:
                    blob_client.upload_blob(
                        data,
                        overwrite=True,
                        connection_timeout=600,
                        standard_blob_tier=storage_tier.value,
                    )
                if delete_src:
                    logger.debug(f"Deleting uploaded file: {file}")
                    file.unlink()
            else:
                logger.error(f"{root_cfg.RAISE_WARN()}Upload failed because file {file} does not exist")

    def download_from_container(self, src_container: str, src_file: str, dst_file: Path) -> None:
        """Downloads the src_datafile to a local dst_file Path."""
        download_container = self._validate_container(src_container)

        blob_client = download_container.get_blob_client(src_file)
        self._download_blob(blob_client, dst_file)

    def download_container(
        self,
        src_container: str,
        dst_dir: Path,
        folder_prefix_len: int | None = None,
        files: list[str] | None = None,
        overwrite: bool | None = True,
    ) -> None:
        """Download all the files in the src_container to the dst_dir.

        Parameters:
            src_container: source container
            dst_dir: destination directory to download the files to
            folder_prefix_len: Optional; first n characters of the file name to use as a subfolder
            files: Optional; list of files to download from src_container; if None, all files in the container
                will be downloaded; useful for chunking downloads
            overwrite: Optional; if False, function will skip downloading files that already existing in
                dst_dir
        """
        download_container = self._validate_container(src_container)
        original_dst_dir = dst_dir

        if files is None:
            for blob in download_container.list_blobs():
                blob_client = download_container.get_blob_client(blob.name)
                if folder_prefix_len is not None:
                    dst_dir = original_dst_dir.joinpath(blob.name[:folder_prefix_len])
                    if not dst_dir.exists():
                        dst_dir.mkdir(parents=True, exist_ok=True)
                self._download_blob(blob_client, dst_dir.joinpath(blob.name))
        else:
            files_downloaded = 0
            # Create a pool of threads to download the files
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = []
                for blob_name in files:
                    blob_client = download_container.get_blob_client(blob_name)
                    if folder_prefix_len is not None:
                        dst_dir = original_dst_dir.joinpath(blob_name[:folder_prefix_len])
                    dst_file = dst_dir.joinpath(blob_name)
                    if not overwrite and dst_file.exists():
                        logger.debug(f"File {dst_file} already exists; skipping download")
                        continue
                    futures.append(executor.submit(self._download_file, blob_client, dst_file))
                    if len(futures) > 10000:
                        logger.info("Working on batch of 10000 files")
                        for future in as_completed(futures, timeout=600):
                            future.result()
                            files_downloaded += 1
                        futures = []
                logger.info(f"Downloading total of {len(futures)} files")

                for future in as_completed(futures, timeout=600):
                    future.result()
                    files_downloaded += 1
                logger.info(f"Completed downloaded of {files_downloaded} files")

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
        from_container = self._validate_container(src_container)
        to_container = self._validate_container(dst_container)

        for blob_name in blob_names:
            src_blob = from_container.get_blob_client(blob_name)
            dst_blob = to_container.get_blob_client(blob_name)
            dst_blob.start_copy_from_url(src_blob.url, standard_blob_tier=storage_tier.value)
            if delete_src:
                src_blob.delete_blob()

            logger.debug(
                f"Moved {blob_name} from {from_container.container_name} to {to_container.container_name}"
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
        If the remote file exists, the first line (headers) in the src_file will be dropped
        so that we don't duplicate a header row.

        We need to cope with changes in software versions which may change the headers in the CSV file.
        Each file in the cloud will only ever be written to by one device (ie this one), so we only need to
        check the headers once at start up.
        If this is the first time we're writing to this remote file, but the file already exists, we check
        that the headers in the local file match the headers in the remote file. If they do not match, we
        download the remote file, merge the data to create a coherent set of headers and push the aggregated
        data back to the remote file.
        """
        try:
            logger.debug(f"CloudConnector.append_to_cloud() with delete_src={delete_src} for {src_file}")

            # Read the local file data ready to append
            with src_file.open("r") as file:
                local_lines = file.readlines()
                if len(local_lines) == 1:
                    return False  # No data beyond headers

            result = self._append_data_to_blob(
                dst_container=dst_container,
                dst_file=src_file.name,
                lines_to_append=local_lines,
                col_order=col_order,
            )

            if result and delete_src:
                logger.debug(f"Deleting append file: {src_file}")
                src_file.unlink()

            return result
        except Exception:
            logger.exception(f"{root_cfg.RAISE_WARN()}Failed to append data to {src_file}")
            return False

    def _append_data_to_blob(
        self,
        dst_container: str,
        dst_file: str,
        lines_to_append: list[str],
        col_order: list[str] | None = None,
    ) -> bool:
        try:
            target_container = self._validate_container(dst_container)
            blob_client = target_container.get_blob_client(dst_file)

            if not blob_client.exists():
                # Create the blob and include the Headers
                blob_client.create_append_blob()
                data_to_append = "".join(lines_to_append[:])
                self._validated_append_files.add(dst_file)
            elif dst_file not in self._validated_append_files:
                # It's our first time writing to this file since reboot
                # Validate that the headers match
                if self._headers_match(blob_client, lines_to_append[0]):
                    logger.debug(f"Headers match for {blob_client.blob_name}, appending data")
                    # Drop the Headers in the first line so we don't have repeat header rows
                    data_to_append = "".join(lines_to_append[1:])
                else:
                    logger.warning(
                        f"{root_cfg.RAISE_WARN()}Headers do not match for {dst_file}, "
                        "downloading remote file to merge headers"
                    )
                    # Download the remote file
                    tmp_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
                    self.download_from_container(
                        src_container=dst_container,
                        src_file=dst_file,
                        dst_file=tmp_file,
                    )
                    # Merge the dataframes
                    orig_df = pd.read_csv(tmp_file)
                    append_df = pd.read_csv(io.StringIO("".join(lines_to_append)))
                    merged_df = pd.concat([orig_df, append_df], ignore_index=True)

                    # Generate the CSV data to append (including the headers)
                    csv_buffer = io.StringIO()
                    merged_df.to_csv(csv_buffer, index=False, columns=col_order)
                    data_to_append = csv_buffer.getvalue()
                    tmp_file.unlink()  # Clean up the temporary file

                    # Re-create the append_blob - this replaces the existing file
                    blob_client.create_append_blob()

                # Record that we've validated this file
                self._validated_append_files.add(dst_file)
            else:
                # Drop the Headers in the first line so we don't have repeat header rows
                data_to_append = "".join(lines_to_append[1:])

            # Append the data
            blob_client.append_block(data_to_append)

            return True
        except Exception:
            logger.exception(f"{root_cfg.RAISE_WARN()}Failed to append data to {dst_file}")
            return False

    def container_exists(self, container: str) -> bool:
        """Check if the specified container exists."""
        containerClient = self._validate_container(container)
        return containerClient.exists()

    def create_container(self, container: str) -> None:
        """Create the specified container."""
        self._validate_container(container)

    def exists(self, src_container: str, blob_name: str) -> bool:
        """Check if the specified blob exits."""
        containerClient = self._validate_container(src_container)
        blob_client = containerClient.get_blob_client(blob_name)
        return blob_client.exists()

    def delete(self, container: str, blob_name: str) -> None:
        """Delete specified blob."""
        containerClient = self._validate_container(container)
        blob_client = containerClient.get_blob_client(blob_name)
        blob_client.delete_blob()

    def list_cloud_files(
        self,
        container: str,
        prefix: str | None = None,
        suffix: str | None = None,
        more_recent_than: datetime | None = None,
    ) -> list[str]:
        """Similar to the Path.glob() method but against a cloud container.

        Parameters:
            - container: container name to be searched
            - prefix: prefix to match to files in the container; does not support wildcards
            - suffix: suffix to match to files in the container
            - more_recent_than: Optional; if specified, only files more recent than this date will be returned

        The current backend implementation is the Azure Blobstore which only supports prefix search
        and tag search.
        """
        logger.debug(
            f"list_cloud_files() called with prefix={prefix}, suffix={suffix}, "
            f"more_recent_than={more_recent_than}"
        )
        containerClient = self._validate_container(container)

        files = []
        if prefix is not None:
            files = list(containerClient.list_blob_names(name_starts_with=prefix))
        else:
            files = list(containerClient.list_blob_names())

        if suffix is not None:
            files = [f for f in files if f.endswith(suffix)]

        if more_recent_than is not None:
            files = [f for f in files if file_naming.get_file_datetime(f) > more_recent_than]
        logger.debug(f"list_cloud_files returning {len(files)!s} files")

        return files

    def list_cloud_files_with_details(
        self,
        container: str,
        include_prefixes: set[str] | None = None,
    ) -> list[tuple[str, int, float]]:
        """Return a list of (filename, size_bytes, last_modified) for all blobs in the container.

        Parameters:
            - container: container name to be searched
            - include_prefixes: Optional, if specified, only return details for files that begin with one of
            these prefixes. Take care when one prefix is a substring of another.
        """
        container_client = self._validate_container(container)

        def include_file(fname: str, include_prefixes: set[str] | None) -> bool:
            return not include_prefixes or ("_".join(fname.split("_")[:2]) + "_") in include_prefixes

        return [
            (blob.name, blob.size, blob.last_modified.timestamp())
            for blob in container_client.list_blobs()
            if include_file(blob.name, include_prefixes)
        ]

    def get_blob_modified_time(self, container: str, blob_name: str) -> datetime:
        """Get the last modified time of the specified blob."""
        containerClient = self._validate_container(container)
        blob_client = containerClient.get_blob_client(blob_name)
        if blob_client.exists():
            last_modified = blob_client.get_blob_properties().last_modified
            # The Azure timezone is UTC but it's not explicitly set; set it
            return last_modified.replace(tzinfo=UTC)
        return datetime.min.replace(tzinfo=UTC)

    def shutdown(self) -> None:
        """Shutdown the CloudConnector instance."""
        logger.debug("Shutting down CloudConnector")
        # No resources to release in this implementation, but we could close any open connections if needed
        self._validated_containers.clear()
        CloudConnector._instance = None

    ##########################################################################################################
    # Private utility methods
    ##########################################################################################################
    def _download_blob(self, blob_client: BlobClient, dst_file: Path) -> None:
        with open(dst_file, "wb") as my_file:
            download_stream = blob_client.download_blob()
            my_file.write(download_stream.readall())  # type: ignore[invalid-argument-type]

    def _download_file(self, blob_client: BlobClient, dst_file: Path) -> str:
        """Download a single file."""
        if not dst_file.parent.exists():
            dst_file.parent.mkdir(parents=True, exist_ok=True)
        self._download_blob(blob_client, dst_file)
        return dst_file.name

    def _validate_container(self, container: str) -> ContainerClient:
        """Validate a container string or ContainerClient and return a ContainerClient instance.

        Create the container if it does not already exist.
        """
        container_client = ContainerClient.from_connection_string(
            conn_str=self._get_connection_string(), container_name=container
        )

        # Only do an external check if we haven't already validated this container
        if container_client.container_name not in self._validated_containers:
            if not container_client.exists():
                logger.info(f"Creating container {container}")
                container_client.create_container()
            self._validated_containers[container_client.container_name] = container_client

        return container_client

    def _get_connection_string(self) -> str:
        return self._connection_string

    def _headers_match(self, blob_client: BlobClient, local_line: str) -> bool:
        """Check if the headers in the local file match the headers in the remote file.

        Returns false if either is empty or if the headers do not match.
        """
        start_of_contents = blob_client.download_blob(encoding="utf-8").read(chars=1000)

        if not start_of_contents:
            return False  # No contents in the remote file

        if not local_line.strip():
            logger.warning(f"{root_cfg.RAISE_WARN()}Local file {blob_client.blob_name} has no headers")
            return False  # No headers in the local file

        # Get the first line from start_of_contents
        cloud_lines = start_of_contents.splitlines()
        if len(cloud_lines) >= 1:
            # We have headers from local and cloud files; check headers match
            local_reader = csv.reader([local_line])
            cloud_reader = csv.reader([cloud_lines[0]])  # type: ignore
            local_headers = next(local_reader)
            cloud_headers = next(cloud_reader)
            if local_headers != cloud_headers:
                logger.warning(
                    f"{root_cfg.RAISE_WARN()}Local and remote headers do not match in "
                    f"{blob_client.blob_name}: {local_headers}, {cloud_headers}"
                )
                return False

            # All is good; headers match
            logger.debug(f"Headers match for {blob_client.blob_name}: {local_headers}")
            return True

        logger.warning(f"{root_cfg.RAISE_WARN()}Remote file {blob_client.blob_name} has no headers")
        return False
