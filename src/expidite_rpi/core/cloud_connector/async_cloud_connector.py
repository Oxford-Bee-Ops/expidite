import contextlib
import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
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
from expidite_rpi.core.cloud_connector.spool import DiskSpool, SpooledAppend, SpooledUpload, SpoolResult

logger = root_cfg.setup_logger("expidite")

# When both the network attempt and the spool write fail (doubly-degraded device: no connectivity AND an
# unwritable spool disk), the item is re-queued for another cycle; this delay stops that cycle from
# spinning hot (a DNS failure can return in milliseconds).
_SPOOL_FALLBACK_BACKOFF_SECONDS = 10.0

# A spooled item that fails this many drain attempts with a non-transient error is quarantined so it
# cannot block the items spooled behind it forever (e.g. a destination append blob at Azure's block limit).
_DRAIN_MAX_ITEM_FAILURES = 5


##############################################################################################################
# AsyncCloudConnector class
# This class uses async methods to *UPLOAD* files to the cloud storage provider (Azure Blob Storage).
# This improves resilience to transient network issues and reduces data loss.
# Download / exists / list methods are *not* asynchronous and use the default CloudConnector.
#
# Store-and-forward on failure: each queued item gets exactly ONE network attempt on a worker thread. On
# success it's done; on ANY failure the data is persisted to the DiskSpool (real disk, survives reboots)
# and the drain thread becomes the only retry mechanism - it uploads the spool oldest-first every
# SPOOL_DRAIN_INTERVAL, stopping a pass on the first transient network failure (so the first item doubles
# as the connectivity probe) and quarantining items that repeatedly fail non-transiently. This bounds RAM:
# during an outage each item transits the queue once and then lives on disk, so a device can run for a
# week offline without memory growth, and a reboot loses nothing that reached the spool. Shutdown never
# touches the network: the queue is spilled to the spool and in-flight uploads are safety-copied, so
# shutdown is fast and safe regardless of connectivity; the spool drains after the next start.
#
# The offline/online state (10 minutes of continuous transient failures) is operator telemetry only - it
# drives the customer-facing "entering offline mode" fault and is_offline(); no data path depends on it.
##############################################################################################################
# eq=False: actions are tracked by identity in the in-flight set, so they must hash by identity.
@dataclass(eq=False)
class AsyncUpload:
    """Class to hold the action to be performed on the cloud."""

    dst_container: str
    src_files: list[Path]
    delete_src: bool
    storage_tier: api.StorageTier = api.StorageTier.HOT
    # When True the files are never written to the disk spool: if the single live upload attempt fails
    # (or spooling is otherwise triggered by memory pressure/shutdown) the data is simply dropped. Used
    # for recordings the caller has declared expendable, so they never consume scarce spool disk.
    can_discard: bool = False


# eq=False: actions are tracked by identity in the in-flight set, so they must hash by identity.
@dataclass(eq=False)
class AsyncAppend:
    """Class to hold the action to be performed on the cloud."""

    dst_container: str
    src_fname: str
    delete_src: bool
    data: list[str]
    col_order: list[str] | None = None


class _DrainOutcome(Enum):
    OK = "ok"  # uploaded (or vanished - nothing left to do); keep draining
    TRANSIENT = "transient"  # network down; abort this pass, retry next tick
    FAILED = "failed"  # non-transient per-item failure; skip it and keep draining


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
        # Offline telemetry state. _first_transient_failure is when cloud calls started failing with
        # transient network errors; once that persists for SPOOL_OFFLINE_AFTER_SECONDS we report offline
        # mode (a customer-facing fault). Any successful cloud call resets both. Purely informational: the
        # data path (attempt once -> spool on failure) is the same online and offline.
        self._spool = DiskSpool()
        self._state_lock = Lock()
        self._offline = False
        self._first_transient_failure: float | None = None
        self._last_memory_log = 0.0
        # Queue items currently being processed by a worker thread; used at shutdown to safety-copy their
        # data to the spool in case the process is killed mid-upload.
        self._in_flight: set[AsyncAppend | AsyncUpload] = set()
        # Per-item drain failure counts (drain thread only; no lock needed) driving quarantine.
        self._drain_failures: dict[Path, int] = {}
        # Start the worker thread to process the upload queue
        self._worker_pool.submit(self.do_work)
        # Start the spool drain thread. Daemon so a drain stuck in a long network timeout can never block
        # process exit. Woken immediately if a previous run left data in the spool (e.g. across a reboot).
        self._drain_wake = Event()
        if self._spool.has_data():
            logger.info("Disk spool contains data from a previous run; drain scheduled")
            self._drain_wake.set()
        self._drain_thread = Thread(target=self._drain_loop, name="spool_drain", daemon=True)
        self._drain_thread.start()

    def shutdown(self) -> None:
        """Shutdown the connector; queued and in-flight upload data is persisted to the disk spool.

        No network I/O is attempted, so shutdown is fast and bounded regardless of connectivity - safe to
        run mid-outage or during a system reboot. Data still in transit lands in Azure via the drain after
        the next start instead of before this shutdown; that latency is the price of a simple, reliable
        stop path.
        """
        if self._stop_requested.is_set():
            return
        logger.debug("AsyncCloudConnector.shutdown() called")
        self._stop_requested.set()
        self._drain_wake.set()
        # Wake do_work if it is blocked on an empty queue; it exits on the sentinel (or spools anything it
        # dequeues once stop is requested).
        self._upload_queue.put(None)
        self._spill_queue_to_spool()
        # Safety-copy the data of uploads still running on worker threads: if the process is killed
        # mid-upload the data is on disk rather than lost with the tmpfs; if the upload completes anyway,
        # the next drain re-uploads the same blob names (harmless overwrite / accepted duplicate rows).
        with self._state_lock:
            in_flight_actions = list(self._in_flight)
        for action in in_flight_actions:
            self._spool_action(action, safety_copy=True)

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
        can_discard: bool = False,
    ) -> None:
        """Async version of upload_to_container using a queue and thread pool for parallel uploads.

        can_discard=True marks these files as expendable: on any upload failure they are dropped rather
        than persisted to the disk spool (see _spool_action), so declared-expendable recordings never take
        up scarce spool disk during an outage.
        """
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
            self._upload_queue.put(
                AsyncUpload(dst_container, src_files, delete_src, storage_tier, can_discard)
            )

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

    def is_offline(self) -> bool:
        """True when cloud uploads have been failing with transient network errors for a sustained period.

        Telemetry only: the data path is identical online and offline (attempt once, spool on failure).
        """
        with self._state_lock:
            return self._offline

    ##########################################################################################################
    # Offline telemetry & memory pressure
    ##########################################################################################################
    def _memory_pressure(self) -> bool:
        """True when system memory usage is high enough that queued data should go straight to disk.

        Normally RAM is bounded because each item transits the queue only once - but workers stuck in long
        network timeouts (a black-holing network rather than a fast-failing one) can back the queue up.
        Only applied on RPi: development machines routinely run at high memory usage.
        """
        if not root_cfg.running_on_rpi:
            return False
        return psutil.virtual_memory().percent > root_cfg.SPOOL_AT_MEMORY_PERCENT

    def _note_cloud_failure(self, exc: BaseException) -> None:
        """Track a failed cloud call; report offline mode once transient failures have persisted too long."""
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
                f"{root_cfg.RAISE_WARN()}Persistent network failure; device is offline - data is being "
                f"queued on disk in {self._spool.root}"
            )

    def _note_cloud_success(self) -> None:
        """Track a successful cloud call; report recovery and schedule a spool drain."""
        with self._state_lock:
            was_offline = self._offline
            self._offline = False
            self._first_transient_failure = None
        if was_offline:
            logger.info("Cloud connectivity restored; leaving offline mode")
            self._drain_wake.set()

    def _spill_queue_to_spool(self) -> None:
        """Move everything on the in-memory upload queue to the disk spool. Shutdown path; no network I/O."""
        spilled = 0
        while True:
            try:
                queue_item = self._upload_queue.get_nowait()
            except Empty:
                break
            if queue_item is None:
                # do_work's wake-up sentinel; put it back rather than swallowing it.
                self._upload_queue.put(None)
                break
            if not self._spool_action(queue_item):
                # Spool disk full/unwritable at shutdown: there is no retry path left.
                logger.error(f"{root_cfg.RAISE_WARN()}Spool unavailable at shutdown; data lost: {queue_item}")
            self._upload_queue.task_done()
            spilled += 1
        if spilled:
            logger.info(f"Spilled {spilled} queued uploads to disk spool at {self._spool.root}")

    def _spool_action(self, action: AsyncAppend | AsyncUpload, safety_copy: bool = False) -> bool:
        """Persist one queue item to the disk spool.

        safety_copy=True never moves or deletes source files (used for uploads still in flight on a worker
        thread, whose files may be mid-read and are cleaned up by that worker on success).

        Returns False if any of the action's data could not be persisted (spool disk full/unwritable) and
        the caller still holds it - the caller should then keep the action in RAM rather than lose data.
        A DROPPED video is a deliberate policy decision and counts as handled. For a (non safety-copy)
        upload action, files that did make it to the spool are removed from action.src_files so a fallback
        re-queue retries only the failed ones.

        A discardable upload is never spooled: it is dropped here and reported as handled (True), so an
        expendable recording that failed its one live upload attempt does not consume spool disk.
        """
        if isinstance(action, AsyncAppend):
            return self._spool.spool_append(action.dst_container, action.src_fname, action.data)

        if action.can_discard:
            # safety_copy leaves the in-flight worker's files alone (that worker cleans them up itself);
            # otherwise drop the temp copy we own in upload_to_container.
            if action.delete_src and not safety_copy:
                self._discard_upload_files(action)
            logger.warning(f"Dropped discardable upload for {action.dst_container} / {action.src_files}")
            return True

        remaining = []
        for file in action.src_files:
            if not file.exists():
                continue
            move = action.delete_src and not safety_copy
            result = self._spool.spool_upload(action.dst_container, file, action.storage_tier, move=move)
            if result is SpoolResult.FAILED and file.exists():
                remaining.append(file)

        if action.delete_src and not safety_copy:
            # The files lived in a temporary directory we created in upload_to_container. Files that failed
            # to spool stay in it for the fallback re-queue; once none remain, remove the empty directory.
            tmp_dir = action.src_files[0].parent if action.src_files else None
            action.src_files = remaining
            if (
                not remaining
                and tmp_dir is not None
                and tmp_dir.exists()
                and tmp_dir.is_dir()
                and not any(tmp_dir.iterdir())
            ):
                tmp_dir.rmdir()
        return not remaining

    @staticmethod
    def _discard_upload_files(action: AsyncUpload) -> None:
        """Delete a discardable upload's temp files and the temporary dir upload_to_container created."""
        tmp_dir = action.src_files[0].parent if action.src_files else None
        for file in action.src_files:
            file.unlink(missing_ok=True)
        action.src_files = []
        if tmp_dir is not None and tmp_dir.exists() and tmp_dir.is_dir() and not any(tmp_dir.iterdir()):
            with contextlib.suppress(OSError):
                tmp_dir.rmdir()

    ##########################################################################################################
    # Spool drain thread - the ONLY retry mechanism
    ##########################################################################################################
    def _drain_loop(self) -> None:
        """Periodically drain the disk spool back to the cloud."""
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
        """Drain the spool until it is empty or the network fails.

        The first item of a pass doubles as the connectivity probe: a transient network failure aborts the
        pass, to be retried next tick. A non-transient per-item failure is counted and the item quarantined
        after _DRAIN_MAX_ITEM_FAILURES, so one poison item cannot wedge everything spooled behind it.
        Draining is sequential on this one thread so a large backlog trickles out without starving live
        uploads of the worker pool. Appends first (small CSVs - cheap probes), oldest first.
        """
        drained_any = False
        while not self._stop_requested.is_set():
            items: list[SpooledAppend | SpooledUpload] = []
            items.extend(self._spool.pending_appends())
            items.extend(self._spool.pending_uploads())
            if not items:
                if drained_any:
                    logger.info("Disk spool fully drained")
                return
            progressed = False
            for item in items:
                if self._stop_requested.is_set():
                    return
                outcome = self._drain_item(item)
                if outcome is _DrainOutcome.TRANSIENT:
                    return
                if outcome is _DrainOutcome.OK:
                    progressed = True
                    drained_any = True
            if not progressed:
                # Everything left failed non-transiently this pass; wait for the next tick rather than spin.
                return

    def _drain_item(self, item: SpooledAppend | SpooledUpload) -> _DrainOutcome:
        """Upload one spooled item; remove it from the spool on success."""
        try:
            if not item.path.exists():
                # Stale listing: evicted for budget, or drained by another process's connector. Tidy up but
                # do NOT report a cloud success - no network evidence was gathered.
                self._spool.remove(item)
                self._drain_failures.pop(item.path, None)
                return _DrainOutcome.OK
            if isinstance(item, SpooledAppend):
                with item.path.open("r") as f:
                    lines = f.readlines()
                if lines and not self._append_data_to_blob(
                    dst_container=item.dst_container,
                    dst_file=item.dst_fname,
                    lines_to_append=lines,
                    swallow_exceptions=False,
                ):
                    return self._register_drain_failure(item)
            else:
                super().upload_to_container(
                    item.dst_container, [item.path], delete_src=False, storage_tier=item.storage_tier
                )
        except Exception as e:
            log_cloud_failure(f"Spool drain failed for {item.path.name}", e)
            if is_transient_network_error(e):
                self._note_cloud_failure(e)
                return _DrainOutcome.TRANSIENT
            return self._register_drain_failure(item)
        self._note_cloud_success()
        self._spool.remove(item)
        self._drain_failures.pop(item.path, None)
        logger.debug(f"Drained {item.path.name} from disk spool")
        return _DrainOutcome.OK

    def _register_drain_failure(self, item: SpooledAppend | SpooledUpload) -> _DrainOutcome:
        count = self._drain_failures.get(item.path, 0) + 1
        self._drain_failures[item.path] = count
        if count >= _DRAIN_MAX_ITEM_FAILURES:
            self._drain_failures.pop(item.path, None)
            self._spool.quarantine(item)
        return _DrainOutcome.FAILED

    ##########################################################################################################
    # Worker methods - exactly one network attempt per item; failure means the spool
    ##########################################################################################################
    def _async_upload(
        self,
        action: AsyncUpload,
    ) -> None:
        """Attempt the upload once; on any failure persist the files to the disk spool.

        This method is called on a thread from the ThreadPoolExecutor. The drain thread owns all retries.
        """
        start_time = perf_counter()
        try:
            logger.debug(f"_async_upload with delete_src={action.delete_src} for {action.src_files}")
            super().upload_to_container(
                action.dst_container, action.src_files, action.delete_src, action.storage_tier
            )
            self._note_cloud_success()
            if action.delete_src:
                # We created a temporary directory for the files in upload_to_container - delete it now
                tmp_dir = action.src_files[0].parent
                if tmp_dir.exists() and tmp_dir.is_dir():
                    shutil.rmtree(tmp_dir)
                else:
                    logger.error(f"{root_cfg.RAISE_WARN()}Temporary directory {tmp_dir} does not exist")
        except Exception as e:
            self._note_cloud_failure(e)
            disposition = "discarding (expendable)" if action.can_discard else "diverting to disk spool"
            log_cloud_failure(f"Upload failed for {action.src_files}; {disposition}", e)
            # Files that no longer exist were uploaded (and deleted) before the failure - drop them.
            action.src_files = [file for file in action.src_files if file.exists()]
            if action.src_files and not self._spool_action(action) and not self._stop_requested.is_set():
                # Doubly-degraded (network AND spool disk failed): keep the data in RAM and try the whole
                # cycle again later rather than lose it.
                self._upload_queue.put(action)
                sleep(_SPOOL_FALLBACK_BACKOFF_SECONDS)
        finally:
            self._finish_in_flight(action)
            self._record_async_timing("upload", perf_counter() - start_time)

    def _async_append(
        self,
        action: AsyncAppend,
    ) -> None:
        """Attempt the append once; on any failure persist the data to the disk spool.

        This method is called on a thread from the ThreadPoolExecutor. The drain thread owns all retries.
        """
        succeeded: bool = False
        start_time = perf_counter()
        try:
            logger.debug(f"_async_append for {action.src_fname}")
            succeeded = self._append_data_to_blob(
                dst_container=action.dst_container,
                dst_file=action.src_fname,
                lines_to_append=action.data,
                col_order=action.col_order,
                swallow_exceptions=False,
            )
            if succeeded:
                self._note_cloud_success()
        except Exception as e:
            self._note_cloud_failure(e)
            log_cloud_failure(f"Append failed for {action.src_fname}; diverting to disk spool", e)
        finally:
            if not succeeded and not self._spool_action(action) and not self._stop_requested.is_set():
                # Doubly-degraded (network AND spool disk failed): keep the data in RAM and try the whole
                # cycle again later rather than lose it.
                self._upload_queue.put(action)
                sleep(_SPOOL_FALLBACK_BACKOFF_SECONDS)
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

    def do_work(self) -> None:
        """Process the upload queue: hand each item to the worker pool for its single network attempt."""
        while not self._stop_requested.is_set():
            try:
                queue_item = self._upload_queue.get()

                if queue_item is None:
                    # Shutdown sentinel.
                    logger.debug("Upload queue flushed")
                    self._upload_queue.task_done()
                    break

                if self._stop_requested.is_set():
                    # Shutdown is spilling the queue; spool anything we dequeued ourselves so nothing is
                    # stranded between the queue and the worker pool.
                    self._spool_action(queue_item)
                    self._upload_queue.task_done()
                    continue

                if self._memory_pressure():
                    # RAM is running out (workers may be stuck in long network timeouts while the queue
                    # backs up; the tmpfs working dir also counts). Send this item straight to disk. If the
                    # spool can't take it, fall through to a normal attempt (uploading also frees memory).
                    self._log_memory_pressure_throttled()
                    if self._spool_action(queue_item):
                        self._upload_queue.task_done()
                        continue

                with self._state_lock:
                    self._in_flight.add(queue_item)
                if isinstance(queue_item, AsyncAppend):
                    self._worker_pool.submit(self._async_append, queue_item)
                else:
                    self._worker_pool.submit(self._async_upload, queue_item)

                # Mark the task as done, in the sense that we've scheduled it for processing
                self._upload_queue.task_done()
            except Exception:
                logger.exception(f"{root_cfg.RAISE_WARN()}Error during do_work execution")

        logger.info("do_work completed")

    def _log_memory_pressure_throttled(self) -> None:
        """Log memory-pressure diversion at most once a minute - it applies per item and would spam."""
        now = perf_counter()
        if now - self._last_memory_log > 60:
            self._last_memory_log = now
            logger.warning(
                f"Memory usage above {root_cfg.SPOOL_AT_MEMORY_PERCENT}%; diverting queue to disk spool"
            )
