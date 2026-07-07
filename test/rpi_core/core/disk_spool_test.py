"""Tests for outage data retention: the DiskSpool and the AsyncCloudConnector's store-and-forward path.

The connector gives each upload one network attempt and persists to the disk spool on any failure; the drain
thread is the only retry mechanism. These tests never touch Azure: connector tests monkeypatch the parent
CloudConnector network methods, so they exercise the failure/spool/drain paths deterministically.
"""

import logging
import os
import time
from pathlib import Path

import pytest
from azure.core.exceptions import ServiceRequestError

from expidite_rpi.core import api
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.cloud_connector import AsyncCloudConnector
from expidite_rpi.core.cloud_connector import async_cloud_connector as acc_module
from expidite_rpi.core.cloud_connector.async_cloud_connector import AsyncAppend, AsyncUpload
from expidite_rpi.core.cloud_connector.cloud_connector import CloudConnector
from expidite_rpi.core.cloud_connector.spool import DiskSpool, SpoolResult
from expidite_rpi.example.my_sensor_example import (
    EXAMPLE_FILE_STREAM_INDEX,
    EXAMPLE_SENSOR_CFG,
    ExampleSensor,
)

logger = root_cfg.setup_logger("expidite", level=logging.DEBUG)

CONTAINER = "expidite-upload"
CSV_DATA = ["col1,col2\n", "1,2\n", "3,4\n"]


def make_file(dir_path: Path, name: str, nbytes: int = 20) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    f = dir_path / name
    f.write_bytes(b"x" * nbytes)
    return f


class TestDiskSpool:
    @pytest.mark.unittest
    def test_upload_roundtrip(self, tmp_path: Path) -> None:
        spool = DiskSpool(tmp_path / "spool")
        src = make_file(tmp_path, "V3_d01111111111_test.txt")

        assert spool.spool_upload(CONTAINER, src, api.StorageTier.COOL, move=True) is SpoolResult.SPOOLED
        assert not src.exists(), "move=True should transfer ownership of the file to the spool"
        assert spool.has_data()

        items = spool.pending_uploads()
        assert len(items) == 1
        assert items[0].dst_container == CONTAINER
        assert items[0].storage_tier == api.StorageTier.COOL
        assert items[0].path.read_bytes() == b"x" * 20

        spool.remove(items[0])
        assert not spool.has_data()
        assert spool.size_bytes == 0

    @pytest.mark.unittest
    def test_upload_copy_leaves_source(self, tmp_path: Path) -> None:
        spool = DiskSpool(tmp_path / "spool")
        src = make_file(tmp_path, "V3_d01111111111_test.txt")

        assert spool.spool_upload(CONTAINER, src, api.StorageTier.HOT, move=False) is SpoolResult.SPOOLED
        assert src.exists(), "move=False should leave the caller's file untouched"
        assert len(spool.pending_uploads()) == 1

    @pytest.mark.unittest
    def test_upload_same_name_overwrites(self, tmp_path: Path) -> None:
        # FAIR file names are unique per record, so re-spooling the same name (e.g. a safety copy at
        # shutdown plus the failed upload itself) must not create duplicate entries.
        spool = DiskSpool(tmp_path / "spool")
        src1 = make_file(tmp_path / "a", "V3_d01111111111_test.txt", nbytes=10)
        src2 = make_file(tmp_path / "b", "V3_d01111111111_test.txt", nbytes=30)

        assert spool.spool_upload(CONTAINER, src1, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED
        assert spool.spool_upload(CONTAINER, src2, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED
        items = spool.pending_uploads()
        assert len(items) == 1
        assert items[0].path.stat().st_size == 30
        assert spool.size_bytes == 30

    @pytest.mark.unittest
    def test_append_fragments(self, tmp_path: Path) -> None:
        spool = DiskSpool(tmp_path / "spool")
        dst_fname = "V3_HEART_d01111111111_20260701.csv"

        assert spool.spool_append(CONTAINER, dst_fname, CSV_DATA)
        assert spool.spool_append(CONTAINER, dst_fname, ["col1,col2\n", "5,6\n"])

        items = spool.pending_appends()
        assert len(items) == 2
        assert all(i.dst_container == CONTAINER for i in items)
        assert all(i.dst_fname == dst_fname for i in items)
        # Oldest fragment first, each a complete CSV with headers.
        assert items[0].path.read_text() == "".join(CSV_DATA)

        for item in items:
            spool.remove(item)
        assert not spool.has_data()
        assert not (spool.root / "append" / CONTAINER / dst_fname).exists(), (
            "emptied fragment directories should be pruned"
        )

    @pytest.mark.unittest
    def test_budget_evicts_oldest_video_first(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(root_cfg, "SPOOL_MAX_BYTES", 2500)
        monkeypatch.setattr(root_cfg, "SPOOL_MIN_DISK_FREE_BYTES", 0)
        spool = DiskSpool(tmp_path / "spool")

        old_video = make_file(tmp_path, "V3_old.mp4", nbytes=1000)
        new_video = make_file(tmp_path, "V3_new.mp4", nbytes=1000)
        csv_file = make_file(tmp_path, "V3_data.csv", nbytes=100)

        assert spool.spool_upload(CONTAINER, old_video, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED
        # Backdate the first video so eviction ordering by mtime is deterministic.
        spooled_old = spool.pending_uploads()[0].path
        past = time.time() - 1000
        os.utime(spooled_old, (past, past))

        assert spool.spool_upload(CONTAINER, new_video, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED
        assert spool.spool_upload(CONTAINER, csv_file, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED

        # 2100 bytes spooled; a further 1000-byte video breaches the 2500 budget, so the oldest video is
        # evicted to make room and the CSV survives.
        third_video = make_file(tmp_path, "V3_third.mp4", nbytes=1000)
        assert (
            spool.spool_upload(CONTAINER, third_video, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED
        )

        names = {i.path.name for i in spool.pending_uploads()}
        assert names == {"V3_new.mp4", "V3_data.csv", "V3_third.mp4"}
        assert spool.videos_dropped == 1

    @pytest.mark.unittest
    def test_budget_bins_incoming_video_when_no_room(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(root_cfg, "SPOOL_MAX_BYTES", 50)
        monkeypatch.setattr(root_cfg, "SPOOL_MIN_DISK_FREE_BYTES", 0)
        spool = DiskSpool(tmp_path / "spool")

        video = make_file(tmp_path, "V3_big.mp4", nbytes=1000)
        assert spool.spool_upload(CONTAINER, video, api.StorageTier.HOT, move=True) is SpoolResult.DROPPED
        assert not video.exists(), "move=True should still consume the file when it is dropped"
        assert not spool.has_data()

        # Non-video data is allowed to overshoot SPOOL_MAX_BYTES (it is small and precious).
        csv_file = make_file(tmp_path, "V3_data.csv", nbytes=1000)
        assert spool.spool_upload(CONTAINER, csv_file, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED
        assert len(spool.pending_uploads()) == 1

    @pytest.mark.unittest
    def test_part_files_cleaned_and_ignored(self, tmp_path: Path) -> None:
        root = tmp_path / "spool"
        part_dir = root / "upload" / CONTAINER / "HOT"
        part_dir.mkdir(parents=True)
        # A stale .part is crash debris and must be swept; a fresh .part may be a live write by ANOTHER
        # process sharing the spool directory (e.g. bcli constructing a connector while the service is
        # mid-spool) and must be left alone - but still invisible to the drain listings.
        stale = part_dir / "V3_crashed.mp4.part"
        stale.write_bytes(b"x" * 10)
        past = time.time() - 2 * 3600
        os.utime(stale, (past, past))
        fresh = part_dir / "V3_in_progress.mp4.part"
        fresh.write_bytes(b"x" * 10)

        spool = DiskSpool(root)
        assert not stale.exists(), "old half-written files from a crashed run should be cleaned at startup"
        assert fresh.exists(), "a fresh .part may belong to a live write in another process"
        assert not spool.has_data(), ".part files must be invisible to the drain"

    @pytest.mark.unittest
    def test_has_data_skips_disk_when_known_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The steady-state (online, nothing spooling) drain tick must not walk the SD card every minute:
        # once a walk confirms the spool empty, has_data() answers from the in-memory latch until the next
        # spool write. A restart is covered separately: a fresh DiskSpool starts with the latch unset.
        spool = DiskSpool(tmp_path / "spool")

        walk_count = 0
        real_data_files = spool._data_files

        def counting_data_files() -> object:
            nonlocal walk_count
            walk_count += 1
            return real_data_files()

        monkeypatch.setattr(spool, "_data_files", counting_data_files)

        assert not spool.has_data()  # first call walks the disk and latches empty
        assert walk_count == 1
        assert not spool.has_data()  # subsequent calls are answered from the latch, no disk walk
        assert not spool.has_data()
        assert walk_count == 1, "has_data() must not touch the disk once the spool is known-empty"

        # A spool write clears the latch, so the very next has_data() re-checks the disk and sees the data.
        # (spool_append may itself walk once for its budget census; snapshot around the has_data() call so
        # this asserts specifically that has_data() re-walked rather than answering stale-empty.)
        assert spool.spool_append(CONTAINER, "V3_HEART_d01111111111_20260701.csv", CSV_DATA)
        walks_before = walk_count
        assert spool.has_data()
        assert walk_count == walks_before + 1, "a spool write must force the next has_data() to re-walk"

    @pytest.mark.unittest
    def test_quarantine_preserves_data_out_of_drain_view(self, tmp_path: Path) -> None:
        spool = DiskSpool(tmp_path / "spool")
        src = make_file(tmp_path, "V3_poison.csv")
        assert spool.spool_upload(CONTAINER, src, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED

        item = spool.pending_uploads()[0]
        spool.quarantine(item)

        assert not spool.has_data(), "quarantined items must be invisible to the drain"
        quarantined = list((spool.root / "quarantine").rglob("V3_poison.csv"))
        assert len(quarantined) == 1, "the data must be preserved for manual recovery"


class TestAsyncCloudConnectorSpool:
    """Store-and-forward behaviour of the AsyncCloudConnector, with all network methods patched out."""

    def _make_cc(self, tmp_path: Path) -> AsyncCloudConnector:
        cc = AsyncCloudConnector()
        cc._spool = DiskSpool(tmp_path / "spool")
        return cc

    def _stop_background_threads(self, cc: AsyncCloudConnector) -> None:
        """Stop the do_work and drain threads so a test can drive the connector deterministically."""
        cc._stop_requested.set()
        cc._drain_wake.set()
        cc._upload_queue.put(None)
        cc._drain_thread.join(timeout=5)
        time.sleep(0.2)  # Let do_work consume the sentinel and exit
        cc._stop_requested.clear()

    def _staged_upload(
        self, tmp_path: Path, name: str = "V3_d01111111111_test.txt", can_discard: bool = False
    ) -> AsyncUpload:
        """Build an AsyncUpload as upload_to_container would: file staged in its own tmp dir."""
        staged = make_file(tmp_path / f"staging_{name}", name)
        return AsyncUpload(CONTAINER, [staged], delete_src=True, can_discard=can_discard)

    @pytest.mark.unittest
    def test_offline_after_persistent_transient_failures(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(root_cfg, "SPOOL_OFFLINE_AFTER_SECONDS", 0.0)
        cc = self._make_cc(tmp_path)
        try:
            # Non-transient errors never trigger offline mode.
            cc._note_cloud_failure(ValueError("bad data"))
            cc._note_cloud_failure(ValueError("bad data"))
            assert not cc.is_offline()

            # The first transient failure starts the clock; once the threshold has elapsed (0s here) the
            # next failure flips us offline.
            cc._note_cloud_failure(ServiceRequestError("name resolution failure"))
            assert not cc.is_offline()
            cc._note_cloud_failure(ServiceRequestError("name resolution failure"))
            assert cc.is_offline()

            cc._note_cloud_success()
            assert not cc.is_offline()
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_worker_failure_spools_upload(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # The core store-and-forward contract: one network attempt, then the data is on disk.
        cc = self._make_cc(tmp_path)
        self._stop_background_threads(cc)
        try:

            def failing_upload(self: CloudConnector, *args: object, **kwargs: object) -> None:
                raise ServiceRequestError(message="name resolution failure")

            monkeypatch.setattr(CloudConnector, "upload_to_container", failing_upload)

            action = self._staged_upload(tmp_path)
            staging_dir = action.src_files[0].parent
            cc._async_upload(action)

            assert cc._upload_queue.empty(), "a failed upload must not be re-queued in RAM"
            assert len(cc._spool.pending_uploads()) == 1
            assert not staging_dir.exists(), "the tmpfs staging dir should be cleaned up after spooling"
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_discardable_upload_dropped_not_spooled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # can_discard marks a recording as expendable: a failed network attempt drops it rather than
        # persisting it to the spool, so it never consumes scarce spool disk during an outage.
        cc = self._make_cc(tmp_path)
        self._stop_background_threads(cc)
        try:

            def failing_upload(self: CloudConnector, *args: object, **kwargs: object) -> None:
                raise ServiceRequestError(message="name resolution failure")

            monkeypatch.setattr(CloudConnector, "upload_to_container", failing_upload)

            action = self._staged_upload(tmp_path, can_discard=True)
            staging_dir = action.src_files[0].parent
            cc._async_upload(action)

            assert cc._upload_queue.empty(), "a discardable upload must not be re-queued in RAM"
            assert not cc._spool.has_data(), "a discardable upload must not be written to the spool"
            assert not staging_dir.exists(), "the tmpfs staging dir must be cleaned up after discarding"
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_worker_failure_spools_append(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        cc = self._make_cc(tmp_path)
        self._stop_background_threads(cc)
        try:

            def failing_append(self: CloudConnector, **kwargs: object) -> bool:
                raise ServiceRequestError(message="name resolution failure")

            monkeypatch.setattr(CloudConnector, "_append_data_to_blob", failing_append)

            cc._async_append(AsyncAppend(CONTAINER, "V3_HEART_d01111111111_20260701.csv", True, CSV_DATA))

            assert cc._upload_queue.empty(), "a failed append must not be re-queued in RAM"
            items = cc._spool.pending_appends()
            assert len(items) == 1
            assert items[0].path.read_text() == "".join(CSV_DATA)
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_memory_pressure_diverts_to_spool(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Under memory pressure, do_work sends dequeued items straight to disk without a network attempt.
        cc = self._make_cc(tmp_path)
        try:
            monkeypatch.setattr(cc, "_memory_pressure", lambda: True)
            src = make_file(tmp_path, "V3_d01111111111_test.txt")
            cc.upload_to_container(CONTAINER, [src], delete_src=True)

            deadline = time.monotonic() + 5
            while not cc._spool.has_data() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert len(cc._spool.pending_uploads()) == 1
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_upload_falls_back_to_queue_when_spool_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Doubly-degraded: network down AND spool unwritable - the data must go back to the RAM queue
        # rather than being dropped (with a backoff so the cycle can't spin hot).
        cc = self._make_cc(tmp_path)
        self._stop_background_threads(cc)
        try:
            monkeypatch.setattr(acc_module, "_SPOOL_FALLBACK_BACKOFF_SECONDS", 0.0)

            def failing_upload(self: CloudConnector, *args: object, **kwargs: object) -> None:
                raise ServiceRequestError(message="name resolution failure")

            monkeypatch.setattr(CloudConnector, "upload_to_container", failing_upload)
            monkeypatch.setattr(DiskSpool, "spool_upload", lambda *_args, **_kwargs: SpoolResult.FAILED)

            action = self._staged_upload(tmp_path)
            cc._async_upload(action)

            assert not cc._upload_queue.empty(), "spool failure must fall back to the in-memory queue"
            item = cc._upload_queue.get_nowait()
            assert isinstance(item, AsyncUpload)
            assert item.src_files[0].exists(), "the file must still exist for the queued retry"
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_append_falls_back_to_queue_when_spool_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cc = self._make_cc(tmp_path)
        self._stop_background_threads(cc)
        try:
            monkeypatch.setattr(acc_module, "_SPOOL_FALLBACK_BACKOFF_SECONDS", 0.0)

            def failing_append(self: CloudConnector, **kwargs: object) -> bool:
                raise ServiceRequestError(message="name resolution failure")

            monkeypatch.setattr(CloudConnector, "_append_data_to_blob", failing_append)
            monkeypatch.setattr(DiskSpool, "spool_append", lambda *_args, **_kwargs: False)

            cc._async_append(AsyncAppend(CONTAINER, "V3_HEART_d01111111111_20260701.csv", True, CSV_DATA))

            assert not cc._upload_queue.empty(), "spool failure must fall back to the in-memory queue"
            item = cc._upload_queue.get_nowait()
            assert isinstance(item, AsyncAppend)
            assert item.data == CSV_DATA
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_drain_uploads_spool_and_returns_online(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cc = self._make_cc(tmp_path)
        try:
            self._stop_background_threads(cc)
            with cc._state_lock:
                cc._offline = True

            src = make_file(tmp_path, "V3_d01111111111_test.txt")
            assert cc._spool.spool_upload(CONTAINER, src, api.StorageTier.HOT, move=True)
            assert cc._spool.spool_append(CONTAINER, "V3_HEART_d01111111111_20260701.csv", CSV_DATA)

            uploaded: list[str] = []
            appended: list[str] = []

            def fake_upload(
                self: CloudConnector,
                dst_container: str,
                src_files: list[Path],
                delete_src: bool,
                storage_tier: api.StorageTier = api.StorageTier.HOT,
            ) -> None:
                uploaded.extend(f.name for f in src_files)

            def fake_append(self: CloudConnector, **kwargs: object) -> bool:
                appended.append(str(kwargs["dst_file"]))
                return True

            monkeypatch.setattr(CloudConnector, "upload_to_container", fake_upload)
            monkeypatch.setattr(CloudConnector, "_append_data_to_blob", fake_append)

            cc._drain_spool_once()

            assert not cc.is_offline(), "a successful drain should flip the connector back online"
            assert not cc._spool.has_data()
            assert uploaded == ["V3_d01111111111_test.txt"]
            assert appended == ["V3_HEART_d01111111111_20260701.csv"]
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_drain_stops_on_transient_failure_and_keeps_data(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cc = self._make_cc(tmp_path)
        try:
            self._stop_background_threads(cc)
            src = make_file(tmp_path, "V3_d01111111111_test.txt")
            assert cc._spool.spool_upload(CONTAINER, src, api.StorageTier.HOT, move=True)

            def failing_upload(self: CloudConnector, *args: object, **kwargs: object) -> None:
                raise ServiceRequestError(message="still offline")

            monkeypatch.setattr(CloudConnector, "upload_to_container", failing_upload)

            cc._drain_spool_once()

            assert len(cc._spool.pending_uploads()) == 1, "the spooled file must survive a failed drain"
            assert cc._spool.has_data(), (
                "a failed drain must not latch the spool known-empty; the next tick must re-check the SD "
                "card and retry even though nothing new was spooled"
            )
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_drain_quarantines_poison_item(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # A non-transient per-item failure (e.g. destination blob at Azure's block limit) must not block
        # the items spooled behind it, and must eventually be quarantined rather than retried forever.
        cc = self._make_cc(tmp_path)
        try:
            self._stop_background_threads(cc)
            poison = make_file(tmp_path, "V3_poison.txt")
            good = make_file(tmp_path, "V3_good.txt")
            assert cc._spool.spool_upload(CONTAINER, poison, api.StorageTier.HOT, move=True)
            assert cc._spool.spool_upload(CONTAINER, good, api.StorageTier.HOT, move=True)

            uploaded: list[str] = []

            def picky_upload(
                self: CloudConnector,
                dst_container: str,
                src_files: list[Path],
                delete_src: bool,
                storage_tier: api.StorageTier = api.StorageTier.HOT,
            ) -> None:
                if "poison" in src_files[0].name:
                    msg = "destination blob is full"
                    raise ValueError(msg)
                uploaded.extend(f.name for f in src_files)

            monkeypatch.setattr(CloudConnector, "upload_to_container", picky_upload)

            for _ in range(acc_module._DRAIN_MAX_ITEM_FAILURES + 1):
                cc._drain_spool_once()
                if not cc._spool.has_data():
                    break

            assert uploaded == ["V3_good.txt"], "the good item must drain despite the poison item"
            assert not cc._spool.has_data(), "the poison item must eventually leave the pending listings"
            quarantined = list((cc._spool.root / "quarantine").rglob("V3_poison.txt"))
            assert len(quarantined) == 1, "the poison item's data must be preserved in quarantine"
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_shutdown_spills_queue_to_spool(self, tmp_path: Path) -> None:
        # Shutdown never touches the network: everything queued must land on disk, fast.
        cc = self._make_cc(tmp_path)
        self._stop_background_threads(cc)

        action = self._staged_upload(tmp_path)
        staging_dir = action.src_files[0].parent
        cc._upload_queue.put(action)
        cc._upload_queue.put(AsyncAppend(CONTAINER, "V3_HEART_d01111111111_20260701.csv", True, CSV_DATA))

        start = time.monotonic()
        cc.shutdown()
        elapsed = time.monotonic() - start

        assert elapsed < 10, f"shutdown must be fast and network-free (took {elapsed:.1f}s)"
        assert len(cc._spool.pending_uploads()) == 1
        assert len(cc._spool.pending_appends()) == 1
        assert not staging_dir.exists(), "the empty staging dir should be cleaned up"

    @pytest.mark.unittest
    def test_startup_drains_spool_from_previous_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Simulate the post-reboot case: data is already in the spool when the connector starts.
        spool_root = tmp_path / "spool"
        seed = DiskSpool(spool_root)
        src = make_file(tmp_path, "V3_d01111111111_test.txt")
        assert seed.spool_upload(CONTAINER, src, api.StorageTier.HOT, move=True) is SpoolResult.SPOOLED

        uploaded: list[str] = []

        def fake_upload(
            self: CloudConnector,
            dst_container: str,
            src_files: list[Path],
            delete_src: bool,
            storage_tier: api.StorageTier = api.StorageTier.HOT,
        ) -> None:
            uploaded.extend(f.name for f in src_files)

        monkeypatch.setattr(CloudConnector, "upload_to_container", fake_upload)
        monkeypatch.setattr(root_cfg, "SPOOL_DIR", spool_root)

        cc = AsyncCloudConnector()
        try:
            # The constructor arms the drain thread immediately when the spool holds data.
            deadline = time.monotonic() + 10
            while cc._spool.has_data() and time.monotonic() < deadline:
                time.sleep(0.1)
            assert not cc._spool.has_data(), "spool from a previous run should drain at startup"
            assert uploaded == ["V3_d01111111111_test.txt"]
        finally:
            cc.shutdown()


class _RecordingConnectorStub:
    """Captures upload_to_container calls so tests can inspect exactly what dp_node passes down."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def upload_to_container(
        self,
        dst_container: str,
        src_files: list[Path],
        delete_src: bool,
        storage_tier: api.StorageTier = api.StorageTier.HOT,
        can_discard: bool = False,
    ) -> None:
        self.calls.append(
            {
                "dst_container": dst_container,
                "src_files": list(src_files),
                "delete_src": delete_src,
                "storage_tier": storage_tier,
                "can_discard": can_discard,
            }
        )


class TestSaveRecordingSpoolPlumbing:
    """The dp_node -> connector plumbing for can_discard.

    The connector-level tests above construct AsyncUpload directly, so they cannot detect a bug in
    save_recording's plumbing (e.g. an override forcing every recording discardable). These tests drive a
    real DPnode (ExampleSensor) through save_recording and assert what actually reaches the connector.
    """

    def _make_sensor(self, monkeypatch: pytest.MonkeyPatch) -> tuple[ExampleSensor, _RecordingConnectorStub]:
        # LIVE mode: skip the RpiEmulator recording cap so save_recording runs its real path.
        monkeypatch.setattr(root_cfg, "ST_MODE", root_cfg.SOFTWARE_TEST_MODE.LIVE)
        sensor = ExampleSensor(EXAMPLE_SENSOR_CFG)
        stub = _RecordingConnectorStub()
        # _get_cc() returns self.cc when already set; inject the stub through that public seam.
        monkeypatch.setattr(sensor, "cc", stub)
        root_cfg.EDGE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        return sensor, stub

    @pytest.mark.unittest
    def test_save_recording_is_spoolable_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The default contract: recordings saved without can_discard are precious - on upload failure they
        # must be spooled, so the connector must receive can_discard=False.
        sensor, stub = self._make_sensor(monkeypatch)
        src = tmp_path / "tmp_recording.jpg"
        src.write_bytes(b"jpeg-bytes")

        sensor.save_recording(
            EXAMPLE_FILE_STREAM_INDEX, src, start_time=api.utc_now(), override_sampling=api.OVERRIDE.SAVE
        )

        assert len(stub.calls) == 1
        assert stub.calls[0]["can_discard"] is False, (
            "save_recording without can_discard must reach the connector as NOT discardable, "
            "or every recording is silently dropped instead of spooled during an outage"
        )

    @pytest.mark.unittest
    def test_save_recording_discardable_flag_reaches_connector(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sensor, stub = self._make_sensor(monkeypatch)
        src = tmp_path / "tmp_recording.jpg"
        src.write_bytes(b"jpeg-bytes")

        sensor.save_recording(
            EXAMPLE_FILE_STREAM_INDEX,
            src,
            start_time=api.utc_now(),
            override_sampling=api.OVERRIDE.SAVE,
            can_discard=True,
        )

        assert len(stub.calls) == 1
        assert stub.calls[0]["can_discard"] is True
