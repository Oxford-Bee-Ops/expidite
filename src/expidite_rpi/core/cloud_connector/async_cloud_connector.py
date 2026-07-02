import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock, Thread
from time import perf_counter, sleep

import psutil

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector.cloud_connector import (
    CloudConnector,
    is_transient_network_error,
    log_cloud_failure,
)
from expidite_rpi.core.cloud_connector.spool import DiskSpool, SpooledAppend, SpooledUpload

logger = root_cfg.setup_logger("expidite")


##############################################################################################################
# AsyncCloudConnector class
# This class uses async methods to *UPLOAD* files to the cloud storage provider (Azure Blob Storage).
# This improves resilience to transient network issues and reduces data loss.
# Download / exists / list methods are *not* asynchronous and use the default CloudConnector.
#
# Data retention during outages: the upload queue holds append data in RAM, and upload files sit in TMP_DIR
# (a tmpfs on SD-card devices), so during a prolonged network outage the backlog would exhaust memory and be
# lost entirely on the recovery reboot. To prevent this the connector tracks an online/offline state: after
# SPOOL_OFFLINE_AFTER_SECONDS of continuous transient network failures (or under memory pressure) it diverts
# uploads to a persistent DiskSpool instead of retrying them in memory. A drain thread probes for recovery
# and uploads the spool - including at startup, which is how data spooled before a reboot reaches the cloud.
##############################################################################################################
# eq=False: actions are tracked by identity in the in-flight set, so they must hash by identity.
@dataclass(eq=False)
class AsyncUpload:
    """Class to hold the action to be performed on the cloud."""

    dst_container: str
    src_files: list[Path]
    delete_src: bool
    storage_tier: api.StorageTier = api.StorageTier.HOT
    iteration: int = 0
    # Monotonic time this action was first queued; used to escalate a persistently-failing upload to a
    # fault based on how long it has been failing (see log_cloud_failure). Survives re-queues.
    first_attempt_monotonic: float = field(default_factory=perf_counter)


# eq=False: actions are tracked by identity in the in-flight set, so they must hash by identity.
@dataclass(eq=False)
class AsyncAppend:
    """Class to hold the action to be performed on the cloud."""

    dst_container: str
    src_fname: str
    delete_src: bool
    data: list[str]
    iteration: int = 0
    col_order: list[str] | None = None
    # Monotonic time this action was first queued; used to escalate a persistently-failing append to a
    # fault based on how long it has been failing (see log_cloud_failure). Survives re-queues.
    first_attempt_monotonic: float = field(default_factory=perf_counter)


class AsyncCloudConnector(CloudConnector):
    def __init__(self) -> None:
        logger.debug("Creating AsyncCloudConnector instance")
        super().__init__()
        self._stop_requested = Event()
        self._shutting_down = False
        self._upload_queue: Queue[AsyncAppend | AsyncUpload | None] = Queue()
        self._worker_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=6)
        self._perf_lock = Lock()
        self._perf_report_interval_seconds = 15 * 60
        self._perf_last_report_time = perf_counter()
        self._async_upload_count = 0
        self._async_upload_total_seconds = 0.0
        self._async_append_count = 0
        self._async_append_total_seconds = 0.0
        # Offline/spool state. _first_transient_failure is when cloud calls started failing with transient
        # network errors; once that persists for SPOOL_OFFLINE_AFTER_SECONDS we go offline and divert
        # uploads to the disk spool. Any successful cloud call resets both.
        self._spool = DiskSpool()
        self._state_lock = Lock()
        self._offline = False
        self._first_transient_failure: float | None = None
        # Queue items currently being processed by a worker thread; used at shutdown to safety-copy their
        # data to the spool in case the process is killed mid-upload.
        self._in_flight: set[AsyncAppend | AsyncUpload] = set()
        # Start the worker thread to process the upload queue
        self._worker_pool.submit(self.do_work)
        # Start the spool drain thread. Daemon so a drain stuck in a long network timeout can never block
        # process exit. Woken immediately if a previous run left data in the spool (e.g. across a reboot).
        self._drain_wake = Event()
        if self._spool.has_data():
            logger.info(f"Disk spool contains {self._spool.size_bytes:,} bytes from a previous run")
            self._drain_wake.set()
        self._drain_thread = Thread(target=self._drain_loop, name="spool_drain", daemon=True)
        self._drain_thread.start()

    def shutdown(self) -> None:
        """Shutdown the connector, guaranteeing queued data is either uploaded or on the disk spool.

        If we are online, in-flight and queued uploads are given SPOOL_SHUTDOWN_FLUSH_SECONDS to complete
        over the network; whatever remains (or everything, if offline) is spilled to the disk spool with no
        network I/O, so shutdown is bounded and safe to run during a network outage or system reboot. The
        spool is drained on the next startup.
        """
        logger.debug("AsyncCloudConnector.shutdown() called")
        with self._state_lock:
            if self._shutting_down:
                return
            # From here on, any upload failure diverts to the spool instead of re-queueing (see
            # _should_divert), so the flush below cannot spin on retries.
            self._shutting_down = True

        # Give the queue a bounded window to flush over the network; skip it entirely if offline.
        if not self.is_offline():
            deadline = perf_counter() + root_cfg.SPOOL_SHUTDOWN_FLUSH_SECONDS
            while perf_counter() < deadline:
                with self._state_lock:
                    in_flight = len(self._in_flight)
                if self._upload_queue.empty() and in_flight == 0:
                    break
                sleep(0.2)

        # Stop the do_work loop and spill everything still queued to the spool.
        self._stop_requested.set()
        self._drain_wake.set()
        self._upload_queue.put(None)
        self._spill_queue_to_spool()

        # Safety-copy the data of any still-running uploads to the spool. If they complete after we exit,
        # the next drain re-uploads the same blob names (harmless overwrite / accepted duplicate rows); if
        # the process is killed mid-upload, the data is on disk rather than lost with the tmpfs.
        with self._state_lock:
            in_flight_actions = list(self._in_flight)
        for action in in_flight_actions:
            self._spool_action(action, safety_copy=True)

        # A worker that failed just before _shutting_down was visible may have re-queued its item after the
        # spill above; sweep the queue once more so nothing is left in RAM.
        self._spill_queue_to_spool()

        self._worker_pool.shutdown(wait=False, cancel_futures=True)
        self._drain_thread.join(timeout=5)

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

        # While offline (or under memory pressure) new work goes straight to the disk spool: queueing it
        # would grow RAM, and staging it in TMP_DIR would grow the tmpfs, which is also RAM.
        if self._should_divert():
            for file in src_files:
                self._spool.spool_upload(dst_container, file, storage_tier, move=delete_src)
            return

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

        # While offline (or under memory pressure) persist the data to the disk spool rather than holding
        # it in RAM on the queue.
        if self._should_divert():
            return self._spool.spool_append(dst_container, src_file.name, data)

        self._upload_queue.put(
            AsyncAppend(dst_container, src_file.name, delete_src, data=data, col_order=col_order)
        )

        return True

    def is_offline(self) -> bool:
        """True when persistent network failure has caused uploads to be diverted to the disk spool."""
        with self._state_lock:
            return self._offline

    ##########################################################################################################
    # Offline / spool state management
    ##########################################################################################################
    def _memory_pressure(self) -> bool:
        """True when system memory usage is high enough that queued data should be spilled to disk.

        Both the upload queue and TMP_DIR (tmpfs) consume RAM, so this covers each. Only applied on RPi:
        development machines routinely run at high memory usage.
        """
        if not root_cfg.running_on_rpi:
            return False
        return psutil.virtual_memory().percent > root_cfg.SPOOL_AT_MEMORY_PERCENT

    def _should_divert(self) -> bool:
        """True when data should go to the disk spool rather than the in-memory queue."""
        with self._state_lock:
            if self._offline or self._shutting_down:
                return True
        return self._memory_pressure()

    def _note_cloud_failure(self, exc: BaseException) -> None:
        """Track a failed cloud call; enter offline mode once transient failures have persisted too long."""
        if not is_transient_network_error(exc):
            return
        go_offline = False
        with self._state_lock:
            now = perf_counter()
            if self._first_transient_failure is None:
                self._first_transient_failure = now
            elif (
                not self._offline
                and now - self._first_transient_failure >= root_cfg.SPOOL_OFFLINE_AFTER_SECONDS
            ):
                self._offline = True
                go_offline = True
        if go_offline:
            logger.error(
                f"{root_cfg.RAISE_WARN()}Persistent network failure; entering offline mode - uploads will "
                f"be spooled to disk at {self._spool.root}"
            )
            # Move the backlog out of RAM now, rather than waiting for each retry to fail again.
            self._spill_queue_to_spool()

    def _note_cloud_success(self) -> None:
        """Track a successful cloud call; return to online mode and schedule a spool drain."""
        with self._state_lock:
            was_offline = self._offline
            self._offline = False
            self._first_transient_failure = None
        if was_offline:
            logger.info("Cloud connectivity restored; leaving offline mode")
            self._drain_wake.set()

    def _spill_queue_to_spool(self) -> None:
        """Move everything on the in-memory upload queue to the disk spool. No network I/O."""
        spilled = 0
        while True:
            try:
                queue_item = self._upload_queue.get_nowait()
            except Empty:
                break
            if queue_item is None:
                # Shutdown sentinel for do_work; put it back rather than swallowing it.
                self._upload_queue.put(None)
                break
            self._spool_action(queue_item)
            self._upload_queue.task_done()
            spilled += 1
        if spilled:
            logger.info(f"Spilled {spilled} queued uploads to disk spool at {self._spool.root}")

    def _spool_action(self, action: AsyncAppend | AsyncUpload, safety_copy: bool = False) -> None:
        """Persist one queue item to the disk spool.

        safety_copy=True never moves or deletes source files (used for uploads still in flight on a worker
        thread, whose files may be mid-read and are cleaned up by that worker on success).
        """
        if isinstance(action, AsyncAppend):
            self._spool.spool_append(action.dst_container, action.src_fname, action.data)
            return
        for file in action.src_files:
            if not file.exists():
                continue
            move = action.delete_src and not safety_copy
            self._spool.spool_upload(action.dst_container, file, action.storage_tier, move=move)
        if action.delete_src and not safety_copy:
            # The files lived in a temporary directory we created in upload_to_container; remove it.
            tmp_dir = action.src_files[0].parent
            if tmp_dir.exists() and tmp_dir.is_dir() and not any(tmp_dir.iterdir()):
                tmp_dir.rmdir()

    ##########################################################################################################
    # Spool drain thread
    ##########################################################################################################
    def _drain_loop(self) -> None:
        """Periodically drain the disk spool back to the cloud (and probe for recovery while offline)."""
        while not self._stop_requested.is_set():
            self._drain_wake.wait(timeout=root_cfg.SPOOL_DRAIN_INTERVAL)
            self._drain_wake.clear()
            if self._stop_requested.is_set():
                break
            try:
                self._drain_spool_once()
            except Exception:
                logger.exception(f"{root_cfg.RAISE_WARN()}Error draining disk spool")
        logger.debug("Spool drain thread exiting")

    def _drain_spool_once(self) -> None:
        """Drain the spool until it is empty or an upload fails.

        While offline, only the smallest item is attempted - a cheap connectivity probe. Its success flips
        us back online (via _note_cloud_success) and the drain continues. Draining is sequential on this one
        thread so a large backlog trickles out without starving live uploads of the worker pool.
        """
        if not self._spool.has_data():
            return

        if self.is_offline():
            probe = self._spool.smallest_pending()
            if probe is None or not self._drain_item(probe):
                return

        while not self._stop_requested.is_set() and not self.is_offline():
            items: list[SpooledAppend | SpooledUpload] = []
            items.extend(self._spool.pending_appends())
            items.extend(self._spool.pending_uploads())
            if not items:
                logger.info("Disk spool fully drained")
                return
            for item in items:
                if self._stop_requested.is_set() or not self._drain_item(item):
                    return

    def _drain_item(self, item: SpooledAppend | SpooledUpload) -> bool:
        """Upload one spooled item; remove it from the spool on success."""
        try:
            if isinstance(item, SpooledAppend):
                with item.path.open("r") as f:
                    lines = f.readlines()
                if lines and not self._append_data_to_blob(
                    dst_container=item.dst_container,
                    dst_file=item.dst_fname,
                    lines_to_append=lines,
                    swallow_exceptions=False,
                ):
                    return False
            else:
                super().upload_to_container(
                    item.dst_container, [item.path], delete_src=False, storage_tier=item.storage_tier
                )
        except Exception as e:
            self._note_cloud_failure(e)
            log_cloud_failure(f"Spool drain failed for {item.path.name}", e)
            return False
        self._note_cloud_success()
        self._spool.remove(item)
        logger.debug(f"Drained {item.path.name} from disk spool")
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
            self._note_cloud_success()
            if action.delete_src:
                # We created a temporary directory for the files in upload_to_cloud - delete it now
                tmp_dir = action.src_files[0].parent
                if tmp_dir.exists() and tmp_dir.is_dir():
                    shutil.rmtree(tmp_dir)
                else:
                    logger.error(f"{root_cfg.RAISE_WARN()}Temporary directory {tmp_dir} does not exist")
        except Exception as e:
            self._note_cloud_failure(e)
            # Check all the src_files still exist and drop any that don't
            log_cloud_failure(
                f"Upload failed for {action.src_files} on iteration {action.iteration}",
                e,
                elapsed_seconds=perf_counter() - action.first_attempt_monotonic,
            )
            verified_files = []
            for file in action.src_files:
                if not file.exists():
                    logger.exception(f"{root_cfg.RAISE_WARN()}Upload of file {file} aborted; does not exist")
                else:
                    verified_files.append(file)

            action.src_files = verified_files

            if action.src_files:
                if self._should_divert():
                    # Offline / shutting down / memory pressure: persist to disk instead of retrying in RAM.
                    self._spool_action(action)
                else:
                    # Re-queue the upload if any src_files still exist
                    action.iteration += 1
                    self._upload_queue.put(action)
                    # Back off for a bit before re-trying the upload
                    sleep(2 * action.iteration)
        finally:
            self._finish_in_flight(action)
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
                elapsed_seconds=perf_counter() - action.first_attempt_monotonic,
                swallow_exceptions=False,
            )
            if succeeded:
                self._note_cloud_success()

        except Exception as e:
            self._note_cloud_failure(e)
            log_cloud_failure(
                f"Upload failed for {action.src_fname} on iteration {action.iteration}",
                e,
                elapsed_seconds=perf_counter() - action.first_attempt_monotonic,
            )
        finally:
            if not succeeded:
                if self._should_divert():
                    # Offline / shutting down / memory pressure: persist to disk instead of retrying in RAM.
                    self._spool_action(action)
                elif action.iteration > 100:
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
            self._finish_in_flight(action)
            self._record_async_timing("append", perf_counter() - start_time)

    def _finish_in_flight(self, action: AsyncAppend | AsyncUpload) -> None:
        with self._state_lock:
            self._in_flight.discard(action)

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

                if queue_item is not None and self._memory_pressure():
                    # RAM is running out (the tmpfs working dir also counts); move the whole backlog,
                    # including this item, to persistent disk rather than processing it in memory.
                    logger.warning(
                        f"Memory usage above {root_cfg.SPOOL_AT_MEMORY_PERCENT}%; "
                        "spilling upload queue to disk spool"
                    )
                    self._spool_action(queue_item)
                    self._upload_queue.task_done()
                    self._spill_queue_to_spool()
                    continue

                if isinstance(queue_item, AsyncAppend):
                    with self._state_lock:
                        self._in_flight.add(queue_item)
                    self._worker_pool.submit(self._async_append, queue_item)
                elif isinstance(queue_item, AsyncUpload):
                    with self._state_lock:
                        self._in_flight.add(queue_item)
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
