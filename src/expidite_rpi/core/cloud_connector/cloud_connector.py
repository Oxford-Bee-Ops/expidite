import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Optional

import pandas as pd
from azure.core.exceptions import (
    ResourceModifiedError,
    ResourceNotFoundError,
    ServiceRequestError,
    ServiceResponseError,
)
from azure.storage.blob import BlobClient, ContainerClient

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.configuration import CloudType

logger = root_cfg.setup_logger("expidite")


def is_transient_network_error(exc: BaseException) -> bool:
    """Return True if exc is a transient connectivity failure rather than a real fault.

    A temporary loss of internet on the device surfaces as a network-level Azure error - most commonly a
    DNS resolution failure (ServiceRequestError, e.g. "Temporary failure in name resolution") or the
    service failing to respond in time (ServiceResponseError). These are expected during brief outages
    and the data is retried, so they should not be reported to customers as faults.
    """
    return isinstance(exc, (ServiceRequestError, ServiceResponseError))


# How long a transient network failure must keep failing before it stops being treated as a brief,
# self-healing outage and is escalated to a customer-facing fault. Escalation is by elapsed wall-clock
# time, not retry count, because the time per retry varies wildly: Azure Storage retries each request
# internally 3 times with ~15-24s exponential backoff (~60s total), there is up to a 20s TCP connect
# timeout per attempt, and our own async back-off sleeps sleep(2 * iteration) between retries. A fixed
# retry count would therefore map to an unpredictable wall-clock time (tens of minutes).
# Derived from the offline-mode threshold so the two customer-facing signals stay aligned by construction:
# an operation escalates to a fault at the same moment the device as a whole reports offline mode.
_ESCALATE_AFTER_SECONDS = root_cfg.SPOOL_OFFLINE_AFTER_SECONDS  # 10 minutes


def log_cloud_failure(message: str, exc: Exception, *, elapsed_seconds: float = 0.0) -> None:
    """Log a failed cloud operation, distinguishing a brief outage from a persistent fault.

    A transient network failure (e.g. a DNS resolution failure while the device briefly has no internet)
    is expected and is retried, so while it is still brief we log it as a concise warning - no stack
    trace, no RAISE_WARN tag - and it is not surfaced to customers as a fault. ``elapsed_seconds`` is how
    long this operation has been failing (across all retries); once it reaches _ESCALATE_AFTER_SECONDS the
    failure has clearly not resolved on its own and is escalated.

    A persistent transient failure, or any non-transient error, is escalated to a customer-facing fault.
    This is logged as two separate records so a stack trace is never attached to a RAISE_WARN line:
    - a clean RAISE_WARN line (ERROR), with no stack trace, which DeviceHealth surfaces to customers;
    - the full stack trace at WARNING with no RAISE_WARN tag, which stays in the local device log for
      engineers but is NOT forwarded to the customer-facing WARNING datastream (see
      device_health.log_warnings, which only forwards RAISE_WARN-tagged or priority<=3 logs).
    """
    if is_transient_network_error(exc) and elapsed_seconds < _ESCALATE_AFTER_SECONDS:
        # Brief, self-healing outage: a concise warning with no stack trace and no RAISE_WARN tag, so it is
        # not surfaced to customers as a fault. The data is retried.
        logger.warning(f"Temporary network failure: {message}: ({exc!s})")
        return

    detail = "Persistent network failure: " if is_transient_network_error(exc) else ""

    # Customer-facing fault: includes the one-line exception message ({exc!s}) but never the stack trace -
    # a stack trace is only ever attached via exc_info, which this line does not use.
    logger.error(f"{root_cfg.RAISE_WARN()}{detail}{message}: ({exc!s})")
    # Engineer-facing detail: the full stack trace, kept in the local device log only (WARNING level, no
    # RAISE_WARN tag) so it is not forwarded to the customer-facing WARNING datastream.
    logger.warning("Stack trace follows for debugging", exc_info=exc)


# Chunk size for delta (append-only) downloads. Each chunk is fetched as a single ranged GET, which keeps
# the Azure SDK from issuing follow-up requests that carry an If-Match condition - and it is that condition
# that aborts a download when the blob is appended to mid-transfer. Must stay at or below the SDK's
# max_single_get_size (default 32 MiB) so each request stays a single GET. See _download_blob_delta.
_DELTA_CHUNK_BYTES = 8 * 1024 * 1024


##############################################################################################################
# Default implementation of the CloudConnector class and interface definition.
#
# This class is used to connect to the cloud storage provider (Azure Blob Storage) but does so synchronously.
##############################################################################################################
class CloudConnector:
    _instance: Optional["CloudConnector"] = None

    def __init__(self) -> None:
        if root_cfg.keys is None or root_cfg.keys.cloud_storage_key == root_cfg.FAILED_TO_LOAD:
            msg = "Cloud storage credentials not set; cannot connect to cloud"
            raise ValueError(msg)

        self._connection_string = root_cfg.keys.cloud_storage_key
        self._validated_containers: dict[str, ContainerClient] = {}
        self._append_locks: dict[str, Lock] = {}
        self._append_locks_lock = Lock()
        self._validated_append_files: set[str] = set()

    @staticmethod
    def get_instance(cloud_type: CloudType) -> "CloudConnector":
        """We use a factory pattern to offer up alternative types of CloudConnector for accessing different
        cloud storage providers and / or the local emulator.
        """
        from expidite_rpi.core.cloud_connector.async_cloud_connector import AsyncCloudConnector
        from expidite_rpi.core.cloud_connector.local_cloud_connector import LocalCloudConnector
        from expidite_rpi.core.cloud_connector.sync_cloud_connector import SyncCloudConnector

        connector_map: dict[CloudType, type[CloudConnector]] = {
            CloudType.AZURE: AsyncCloudConnector,
            CloudType.LOCAL_EMULATOR: LocalCloudConnector,
            CloudType.SYNC_AZURE: SyncCloudConnector,
        }
        desired_class = connector_map[cloud_type]

        if not isinstance(CloudConnector._instance, desired_class):
            if CloudConnector._instance is not None:
                logger.warning(
                    f"{root_cfg.RAISE_WARN()}Replacing CloudConnector instance with {desired_class.__name__}"
                )
                CloudConnector._instance.shutdown()
            CloudConnector._instance = desired_class()
        return CloudConnector._instance

    @staticmethod
    def shutdown_instance() -> None:
        """Shut down the CloudConnector singleton if one exists."""
        if CloudConnector._instance is not None:
            CloudConnector._instance.shutdown()

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
        can_discard: bool = False,
    ) -> None:
        """Upload a list of local files to an Azure container.

        These will be regular block_blobs and not append_blobs; see append_to_cloud for append_blobs.

        Parameters:
            dst_container: destination ContainerClient
            src_files: list of files to upload to the container
            delete_src: delete the local src_files instances after successful upload; defaults to True
            can_discard: only meaningful for the AsyncCloudConnector - see its override. The synchronous base
                connector has no disk spool, so it ignores this flag (a failed upload raises either way).

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
        download_container = self._validate_container(src_container)
        original_dst_dir = dst_dir

        if files is None:
            for blob in download_container.list_blobs():
                blob_client = download_container.get_blob_client(blob.name)
                if folder_prefix_len is not None:
                    dst_dir = original_dst_dir / blob.name[:folder_prefix_len]
                    if not dst_dir.exists():
                        dst_dir.mkdir(parents=True, exist_ok=True)
                self._download_blob(blob_client, dst_dir / blob.name)
        else:
            files_downloaded = 0
            # Create a pool of threads to download the files
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = []
                for blob_name in files:
                    blob_client = download_container.get_blob_client(blob_name)
                    if folder_prefix_len is not None:
                        dst_dir = original_dst_dir / blob_name[:folder_prefix_len]
                    dst_file = dst_dir / blob_name
                    if not overwrite and dst_file.exists():
                        logger.debug(f"File {dst_file} already exists; skipping download")
                        continue
                    futures.append(executor.submit(self._download_file, blob_client, dst_file))
                    if len(futures) > 10_000:
                        logger.info("Working on batch of 10,000 files")
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
        except Exception as e:
            log_cloud_failure(f"Failed to append data to {src_file}", e)
            return False

    def _get_append_lock(self, dst_file: str) -> Lock:
        """Prevent multiple threads from updating the same file in cloud storage simultaneously."""
        with self._append_locks_lock:
            if dst_file not in self._append_locks:
                self._append_locks[dst_file] = Lock()
            return self._append_locks[dst_file]

    def _append_data_to_blob(
        self,
        dst_container: str,
        dst_file: str,
        lines_to_append: list[str],
        col_order: list[str] | None = None,
        elapsed_seconds: float = 0.0,
        swallow_exceptions: bool = True,
    ) -> bool:
        # swallow_exceptions=False re-raises failures instead of logging them, so the AsyncCloudConnector can
        # classify the exception (transient network outage vs real fault) and divert to the disk spool.
        try:
            target_container = self._validate_container(dst_container)
            blob_client = target_container.get_blob_client(dst_file)

            # Prevent multiple threads from updating the same file in cloud storage simultaneously.
            with self._get_append_lock(dst_file):
                if not blob_client.exists():
                    # Create the blob and include the Headers.
                    blob_client.create_append_blob()
                    data_to_append = "".join(lines_to_append[:])
                elif dst_file in self._validated_append_files:
                    # Drop the Headers in the first line so we don't have repeat header rows.
                    data_to_append = "".join(lines_to_append[1:])
                # It's our first time writing to this file since reboot. Validate that the headers match.
                elif self._headers_match(blob_client, lines_to_append[0]):
                    logger.debug(f"Headers match for {blob_client.blob_name}, appending data")
                    # Drop the Headers in the first line so we don't have repeat header rows.
                    data_to_append = "".join(lines_to_append[1:])
                else:
                    logger.warning(
                        f"{root_cfg.RAISE_WARN()}Headers do not match for {dst_file}, "
                        "downloading remote file to merge headers"
                    )
                    data_to_append = self._merge_local_and_remote(
                        dst_container, dst_file, lines_to_append, col_order
                    )

                    # Re-create the append_blob - this replaces the existing file.
                    if blob_client.get_blob_properties().size == 0:
                        blob_client.delete_blob()
                    blob_client.create_append_blob()

                # Append the data.
                blob_client.append_block(data_to_append.encode("utf-8"))

                # Record that we've validated this file (might already be true).
                self._validated_append_files.add(dst_file)

            return True
        except Exception as e:
            if not swallow_exceptions:
                raise
            log_cloud_failure(f"Failed to append data to {dst_file}", e, elapsed_seconds=elapsed_seconds)
            return False

    def _merge_local_and_remote(
        self,
        dst_container: str,
        dst_file: str,
        lines_to_append: list[str],
        col_order: list[str] | None = None,
    ) -> str:
        # Download the remote file.
        tmp_file = file_naming.get_temporary_filename(api.FORMAT.CSV)
        self.download_from_container(
            src_container=dst_container,
            src_file=dst_file,
            dst_file=tmp_file,
        )
        # Merge the dataframes.
        append_df = pd.read_csv(io.StringIO("".join(lines_to_append)))
        try:
            orig_df = pd.read_csv(tmp_file)
        except pd.errors.EmptyDataError:
            logger.warning(
                f"{root_cfg.RAISE_WARN()}Remote append blob {dst_file} is empty; "
                "recreating with incoming headers"
            )
            orig_df = pd.DataFrame(columns=append_df.columns)
        merged_df = pd.concat([orig_df, append_df], ignore_index=True)

        # Generate the CSV data to append (including the headers).
        csv_buffer = io.StringIO()
        merged_df.to_csv(csv_buffer, index=False, columns=col_order)
        data_to_append = csv_buffer.getvalue()
        tmp_file.unlink()  # Clean up the temporary file.
        return data_to_append

    def container_exists(self, container: str) -> bool:
        """Check if the specified container exists."""
        container_client = self._validate_container(container)
        return container_client.exists()

    def create_container(self, container: str) -> None:
        """Create the specified container."""
        self._validate_container(container)

    def exists(self, src_container: str, blob_name: str) -> bool:
        """Check if the specified blob exits."""
        container_client = self._validate_container(src_container)
        blob_client = container_client.get_blob_client(blob_name)
        return blob_client.exists()

    def delete(self, container: str, blob_name: str) -> None:
        """Delete specified blob."""
        container_client = self._validate_container(container)
        blob_client = container_client.get_blob_client(blob_name)
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
        container_client = self._validate_container(container)

        files = []
        if prefix is not None:
            files = list(container_client.list_blob_names(name_starts_with=prefix))
        else:
            files = list(container_client.list_blob_names())

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
        container_client = self._validate_container(container)
        blob_client = container_client.get_blob_client(blob_name)
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

    def download_container_deltas(
        self,
        src_container: str,
        dst_dir: Path,
        files_with_offsets: dict[str, int],
    ) -> None:
        """Download files from src_container, using byte-range requests for partially-downloaded files.

        This function is similar to download_container. It it only for append-only files, and is optimised to
        minimise bytes transferred, by only downloading the additional portion of each file that exists on
        Azure Blob Storage, but not in the local copy.

        Parameters:
            src_container: source container
            dst_dir: destination directory
            file_offsets: mapping of {blob_name: local_byte_offset}; offset=0 means full download,
                offset>0 means append only the bytes from that offset onwards.
                For files that already exist locally, pass the current local file size as the offset so that
                only the new bytes are transferred. For files that don't exist locally, pass offset 0 so that
                the file is downloaded in full.
        """
        download_container = self._validate_container(src_container)
        files_downloaded = 0

        # Create a pool of threads to download the files
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for blob_name, offset in files_with_offsets.items():
                blob_client = download_container.get_blob_client(blob_name)
                dst_file = dst_dir / blob_name
                futures.append(
                    executor.submit(self._download_file, blob_client, dst_file, offset, append_only=True)
                )
                if len(futures) > 10_000:
                    logger.info("Working on batch of 10,000 files")
                    for future in as_completed(futures, timeout=600):
                        future.result()
                        files_downloaded += 1
                    futures = []
            logger.info(f"Downloading remaining {len(futures)} files")
            for future in as_completed(futures, timeout=600):
                future.result()
                files_downloaded += 1
        logger.info(f"Completed download of {files_downloaded} files")

    def get_last_file_modified_time(
        self,
        dst_container: str,
        file_prefix: str,
    ) -> datetime | None:
        """Get the last modified time for the most recently modified file beginning with file_prefix.

        Parameters:
            dst_dir: destination directory
            file_prefix: Must be all the prefix except for the <date>.csv, for example`V3_HEART_<device_id>`

        Checks for the current day file and then the previous day file. For performance, it does not check
        further back than the previous day.

        Returns None if no matching file is found (within the current/previous day window). This is distinct
        from a connectivity failure, which propagates as an exception from the underlying Azure call.
        """
        target_container = self._validate_container(dst_container)

        today = datetime.now(tz=UTC).date()
        for day in (today, today - timedelta(days=1)):
            name = f"{file_prefix}_{day:%Y%m%d}.csv"
            try:
                props = target_container.get_blob_client(name).get_blob_properties()
            except ResourceNotFoundError:
                continue
            return props.last_modified

        return None

    ##########################################################################################################
    # Private utility methods
    ##########################################################################################################
    def _download_blob(self, blob_client: BlobClient, dst_file: Path) -> None:
        """Download a blob file from Azure.

        If the file changed on Azure while downloading, we'll get ResourceModifiedError. This is expected
        occasionally for journals that get appended regularly.
        """
        for attempt in range(3):
            try:
                with open(dst_file, "wb") as my_file:
                    download_stream = blob_client.download_blob()
                    file_bytes = download_stream.readall()
                    my_file.write(file_bytes)  # type: ignore[ty:invalid-argument-type]
                logger.info(f"Downloaded {dst_file.name}, {len(file_bytes):,} bytes")
                return
            except ResourceModifiedError:
                logger.info(f"ResourceModifiedError on {dst_file} attempt {attempt + 1}")
                if attempt == 2:
                    raise

    def _download_blob_delta(self, blob_client: BlobClient, dst_file: Path, offset: int) -> None:
        """Download an append-only blob into dst_file, fetching only the bytes we don't already have.

        Pass offset=0 to download the whole blob, or offset=<local file size> to append only the new tail
        to an existing partial copy.

        These blobs are append-only (see download_container_deltas), so the bytes in [offset, size) never
        change once written, even while the blob keeps growing. We snapshot the blob's current size and
        fetch [offset, size) as a sequence of single-GET chunks. Because each chunk is a single request,
        the Azure SDK never applies its cross-chunk If-Match consistency check, so a concurrent append
        cannot abort the download - there is no need to discard progress and re-download the whole file
        just because the blob grew while we were reading it. This applies equally to a from-scratch
        download (offset=0) of a large, actively-appended blob.
        """
        size = blob_client.get_blob_properties().size
        if size < offset:
            # The blob is smaller than our local copy, so it was truncated or rewritten rather than
            # appended to. The append-only assumption no longer holds, so re-download the whole blob.
            logger.info(f"{dst_file.name} is smaller than local copy ({size:,} < {offset:,}); re-downloading")
            offset = 0
        if size == offset and offset > 0:
            return  # Existing local copy is already complete; nothing new on Azure.

        # offset == 0 means we have nothing usable locally: truncate/create the file and download in full.
        # Otherwise keep the partial copy and append only the new tail.
        mode = "wb" if offset == 0 else "ab"
        written = 0
        with open(dst_file, mode) as my_file:
            cursor = offset
            while cursor < size:
                length = min(_DELTA_CHUNK_BYTES, size - cursor)
                file_bytes = blob_client.download_blob(offset=cursor, length=length).readall()
                my_file.write(file_bytes)  # type: ignore[ty:invalid-argument-type]
                cursor += len(file_bytes)
                written += len(file_bytes)
        descriptor = "bytes" if offset == 0 else "additional bytes"
        logger.info(f"Downloaded {dst_file.name}, {written:,} {descriptor}")

    def _download_file(
        self, blob_client: BlobClient, dst_file: Path, offset: int = 0, append_only: bool = False
    ) -> str:
        """Download a single file.

        For append-only files (append_only=True, the download_container_deltas path), the whole download -
        including the from-scratch case (offset=0) - goes via the chunked delta path, which is immune to a
        concurrent append aborting the transfer. For general files, a rewrite mid-download must invalidate
        the transfer, so only a true partial copy (offset > 0) uses the delta path.
        """
        if not dst_file.parent.exists():
            dst_file.parent.mkdir(parents=True, exist_ok=True)
        if offset > 0 or append_only:
            self._download_blob_delta(blob_client, dst_file, offset)
        else:
            self._download_blob(blob_client, dst_file)
        return dst_file.name

    def _validate_container(self, container: str) -> ContainerClient:
        """Validate a container string or ContainerClient and return a ContainerClient instance.

        Create the container if it does not already exist.
        """
        if container not in self._validated_containers:
            container_client = ContainerClient.from_connection_string(
                conn_str=self._get_connection_string(), container_name=container
            )
            if not container_client.exists():
                logger.info(f"Creating container {container}")
                container_client.create_container()
            self._validated_containers[container] = container_client

        return self._validated_containers[container]

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
