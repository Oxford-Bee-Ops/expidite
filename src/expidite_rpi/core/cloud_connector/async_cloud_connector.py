import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock
from time import perf_counter, sleep

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector.cloud_connector import CloudConnector

logger = root_cfg.setup_logger("expidite")


##############################################################################################################
# AsyncCloudConnector class
# This class uses async methods to *UPLOAD* files to the cloud storage provider (Azure Blob Storage).
# This improves resilience to transient network issues and reduces data loss.
# Download / exists / list methods are *not* asynchronous and use the default CloudConnector.
##############################################################################################################
@dataclass
class AsyncUpload:
    """Class to hold the action to be performed on the cloud."""

    dst_container: str
    src_files: list[Path]
    delete_src: bool
    storage_tier: api.StorageTier = api.StorageTier.HOT
    iteration: int = 0


@dataclass
class AsyncAppend:
    """Class to hold the action to be performed on the cloud."""

    dst_container: str
    src_fname: str
    delete_src: bool
    data: list[str]
    iteration: int = 0
    col_order: list[str] | None = None


class AsyncCloudConnector(CloudConnector):
    def __init__(self) -> None:
        logger.debug("Creating AsyncCloudConnector instance")
        super().__init__()
        self._stop_requested = Event()
        self._upload_queue: Queue[AsyncAppend | AsyncUpload | None] = Queue()
        self._worker_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=6)
        self._perf_lock = Lock()
        self._perf_report_interval_seconds = 15 * 60
        self._perf_last_report_time = perf_counter()
        self._async_upload_count = 0
        self._async_upload_total_seconds = 0.0
        self._async_append_count = 0
        self._async_append_total_seconds = 0.0
        # Start the worker thread to process the upload queue
        self._worker_pool.submit(self.do_work)

    def shutdown(self) -> None:
        """Shutdown the worker pool. Will wait until scheduled uploads are complete before returning."""
        # Try to schedule any remaining uploads (waits up to 1sec for the queue to be processed)
        logger.debug("AsyncCloudConnector.shutdown() called")
        self.do_work(block=False)

        self._stop_requested.set()
        # Flush the queue by putting an empty object on it
        if self._upload_queue is not None:
            self._upload_queue.put(None)
        if self._worker_pool is not None:
            self._worker_pool.shutdown(wait=True, cancel_futures=True)

        self._log_async_performance(force=True)

        super().shutdown()  # Call the parent shutdown method

    ##########################################################################################################
    # Public methods
    ##########################################################################################################
    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: bool,
        storage_tier: api.StorageTier = api.StorageTier.HOT,
    ) -> None:
        """Async version of upload_to_container using a queue and thread pool for parallel uploads."""
        verified_files = []
        for file in src_files:
            if not file.exists():
                logger.error(f"{root_cfg.RAISE_WARN()}Upload of file {file} aborted; does not exist")
            else:
                verified_files.append(file)

        src_files = verified_files

        if delete_src:
            # Rename the files so that they are effectively deleted from the callers perspective
            # We delete this temporary directory in _async_upload() after the upload is complete
            tmp_dir = file_naming.get_temporary_dir()
            for i, file in enumerate(src_files):
                # Move the files to the tmp_dir
                tmp_file = tmp_dir / file.name
                shutil.move(file, tmp_file)
                src_files[i] = tmp_file

        if src_files:
            self._upload_queue.put(AsyncUpload(dst_container, src_files, delete_src, storage_tier))

    def append_to_cloud(
        self, dst_container: str, src_file: Path, delete_src: bool, col_order: list[str] | None = None
    ) -> bool:
        """Async version of append_to_cloud."""
        logger.debug(f"AsyncCC.append_to_cloud() with delete_src={delete_src} for {src_file}")

        if not src_file.exists():
            logger.error(f"{root_cfg.RAISE_WARN()}Upload failed because file {src_file} does not exist")
            return False

        # Read the local file data ready to append
        data: list[str] = []
        with src_file.open("r") as file:
            data = file.readlines()
            if len(data) == 1:
                return False  # No data beyond headers

        if delete_src:
            # Although this is asynchronous, we need to appear to delete the src_files synchronously
            src_file.unlink()

        self._upload_queue.put(
            AsyncAppend(dst_container, src_file.name, delete_src, data=data, col_order=col_order)
        )

        return True

    ##########################################################################################################
    # Private methods
    ##########################################################################################################
    def _async_upload(
        self,
        action: AsyncUpload,
    ) -> None:
        """A wrapper to handle failure when uploading a file to the cloud asynchronously.
        We re-queue the upload if it fails if the src files still exist.
        This method is called on a thread from the ThreadPoolExecutor.
        """
        start_time = perf_counter()
        try:
            logger.debug(
                f"_async_upload with delete_src={action.delete_src}, "
                f"iteration {action.iteration} for {action.src_files}"
            )
            super().upload_to_container(
                action.dst_container, action.src_files, action.delete_src, action.storage_tier
            )
            if action.delete_src:
                # We created a temporary directory for the files in upload_to_cloud - delete it now
                tmp_dir = action.src_files[0].parent
                if tmp_dir.exists() and tmp_dir.is_dir():
                    shutil.rmtree(tmp_dir)
                else:
                    logger.error(f"{root_cfg.RAISE_WARN()}Temporary directory {tmp_dir} does not exist")
        except Exception as e:
            # Check all the src_files still exist and drop any that don't
            logger.warning(
                f"{root_cfg.RAISE_WARN()}Upload failed for {action.src_files} on iter "
                f"{action.iteration}: {e!s}"
            )
            verified_files = []
            for file in action.src_files:
                if not file.exists():
                    logger.exception(f"{root_cfg.RAISE_WARN()}Upload of file {file} aborted; does not exist")
                else:
                    verified_files.append(file)

            action.src_files = verified_files

            if action.src_files:
                # Re-queue the upload if any src_files still exist
                action.iteration += 1
                self._upload_queue.put(action)
                # Back off for a bit before re-trying the upload
                sleep(2 * action.iteration)
        finally:
            self._record_async_timing("upload", perf_counter() - start_time)

    def _async_append(
        self,
        action: AsyncAppend,
    ) -> None:
        """A wrapper to handle failure when uploading append data to the cloud asynchronously.
        We re-queue the append if it fails.
        This method is called on a thread from the ThreadPoolExecutor.
        """
        succeeded: bool = False
        start_time = perf_counter()
        try:
            logger.debug(f"_async_append iteration {action.iteration} for {action.src_fname}")

            succeeded = self._append_data_to_blob(
                dst_container=action.dst_container,
                dst_file=action.src_fname,
                lines_to_append=action.data,
                col_order=action.col_order,
            )

        except Exception as e:
            logger.warning(
                f"{root_cfg.RAISE_WARN()}Upload failed for {action.src_fname} on iter "
                f"{action.iteration}: {e!s}"
            )
        finally:
            if not succeeded:
                if action.iteration > 100:
                    # Failsafe
                    logger.error(
                        f"{root_cfg.RAISE_WARN()}Upload failed for {action.src_fname}"
                        " too many times; giving up"
                    )
                else:
                    # Re-queue the upload @@@ but only if it was a transient failure!
                    action.iteration += 1
                    self._upload_queue.put(action)
                    # Back off for a bit before re-trying the upload
                    sleep(2 * action.iteration)
            self._record_async_timing("append", perf_counter() - start_time)

    def _record_async_timing(self, method: str, duration_seconds: float) -> None:
        with self._perf_lock:
            if method == "upload":
                self._async_upload_count += 1
                self._async_upload_total_seconds += duration_seconds
            elif method == "append":
                self._async_append_count += 1
                self._async_append_total_seconds += duration_seconds

        self._log_async_performance(force=False)

    def _log_async_performance(self, force: bool) -> None:
        with self._perf_lock:
            now = perf_counter()
            if not force and (now - self._perf_last_report_time) < self._perf_report_interval_seconds:
                return

            upload_count = self._async_upload_count
            upload_total_seconds = self._async_upload_total_seconds
            append_count = self._async_append_count
            append_total_seconds = self._async_append_total_seconds

            if upload_count == 0 and append_count == 0:
                self._perf_last_report_time = now
                return

            upload_avg_ms = (upload_total_seconds / upload_count) * 1000 if upload_count else 0.0
            append_avg_ms = (append_total_seconds / append_count) * 1000 if append_count else 0.0

            logger.info(
                "Async cloud performance (last 15 min): "
                f"_async_upload count={upload_count}, total_s={upload_total_seconds:.3f}, "
                f"avg_ms={upload_avg_ms:.2f}; "
                f"_async_append count={append_count}, total_s={append_total_seconds:.3f}, "
                f"avg_ms={append_avg_ms:.2f}"
            )

            self._perf_last_report_time = now
            self._async_upload_count = 0
            self._async_upload_total_seconds = 0.0
            self._async_append_count = 0
            self._async_append_total_seconds = 0.0

    def do_work(self, block: bool = True) -> None:
        """Process the upload queue."""
        while not self._stop_requested.is_set():
            try:
                queue_item = self._upload_queue.get(block=block, timeout=None if block else 1)

                if isinstance(queue_item, AsyncAppend):
                    self._worker_pool.submit(self._async_append, queue_item)
                elif isinstance(queue_item, AsyncUpload):
                    self._worker_pool.submit(self._async_upload, queue_item)
                else:
                    logger.debug(f"Queue flushed using {queue_item}")
                    assert self._stop_requested.is_set(), "Queue flushed but stop_requested is not set"

                # Mark the task as done, in the sense that we've scheduled it for processing
                self._upload_queue.task_done()
            except Empty:
                assert not block, "Queue.get returned but block is True"
                logger.debug("Shutting down upload queue")
                break
            except Exception:
                logger.exception(f"{root_cfg.RAISE_WARN()}Error during do_work execution on {queue_item}")

        logger.info("do_work completed")
