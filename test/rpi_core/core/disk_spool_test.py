"""Tests for outage data retention: the DiskSpool and the AsyncCloudConnector's offline/spill behaviour.

These tests never touch Azure: connector tests monkeypatch the parent CloudConnector network methods, so
they exercise the offline state machine, the disk spool round-trip and the shutdown spill deterministically.
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
from expidite_rpi.core.cloud_connector.async_cloud_connector import AsyncAppend, AsyncUpload
from expidite_rpi.core.cloud_connector.cloud_connector import CloudConnector
from expidite_rpi.core.cloud_connector.spool import DiskSpool

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

        assert spool.spool_upload(CONTAINER, src, api.StorageTier.COOL, move=True)
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

        assert spool.spool_upload(CONTAINER, src, api.StorageTier.HOT, move=False)
        assert src.exists(), "move=False should leave the caller's file untouched"
        assert len(spool.pending_uploads()) == 1

    @pytest.mark.unittest
    def test_upload_same_name_overwrites(self, tmp_path: Path) -> None:
        # FAIR file names are unique per record, so re-spooling the same name (e.g. a safety copy at
        # shutdown plus the failed upload itself) must not create duplicate entries.
        spool = DiskSpool(tmp_path / "spool")
        src1 = make_file(tmp_path / "a", "V3_d01111111111_test.txt", nbytes=10)
        src2 = make_file(tmp_path / "b", "V3_d01111111111_test.txt", nbytes=30)

        assert spool.spool_upload(CONTAINER, src1, api.StorageTier.HOT, move=True)
        assert spool.spool_upload(CONTAINER, src2, api.StorageTier.HOT, move=True)
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

    @pytest.mark.unittest
    def test_budget_evicts_oldest_video_first(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(root_cfg, "SPOOL_MAX_BYTES", 2500)
        monkeypatch.setattr(root_cfg, "SPOOL_MIN_DISK_FREE_BYTES", 0)
        spool = DiskSpool(tmp_path / "spool")

        old_video = make_file(tmp_path, "V3_old.mp4", nbytes=1000)
        new_video = make_file(tmp_path, "V3_new.mp4", nbytes=1000)
        csv_file = make_file(tmp_path, "V3_data.csv", nbytes=100)

        assert spool.spool_upload(CONTAINER, old_video, api.StorageTier.HOT, move=True)
        # Backdate the first video so eviction ordering by mtime is deterministic.
        spooled_old = spool.pending_uploads()[0].path
        past = time.time() - 1000
        os.utime(spooled_old, (past, past))

        assert spool.spool_upload(CONTAINER, new_video, api.StorageTier.HOT, move=True)
        assert spool.spool_upload(CONTAINER, csv_file, api.StorageTier.HOT, move=True)

        # 2100 bytes spooled; a further 1000-byte video breaches the 2500 budget, so the oldest video is
        # evicted to make room and the CSV survives.
        third_video = make_file(tmp_path, "V3_third.mp4", nbytes=1000)
        assert spool.spool_upload(CONTAINER, third_video, api.StorageTier.HOT, move=True)

        names = {i.path.name for i in spool.pending_uploads()}
        assert names == {"V3_new.mp4", "V3_data.csv", "V3_third.mp4"}
        assert spool.videos_binned == 1

    @pytest.mark.unittest
    def test_budget_bins_incoming_video_when_no_room(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(root_cfg, "SPOOL_MAX_BYTES", 50)
        monkeypatch.setattr(root_cfg, "SPOOL_MIN_DISK_FREE_BYTES", 0)
        spool = DiskSpool(tmp_path / "spool")

        video = make_file(tmp_path, "V3_big.mp4", nbytes=1000)
        assert not spool.spool_upload(CONTAINER, video, api.StorageTier.HOT, move=True)
        assert not video.exists(), "move=True should still consume the file when it is binned"
        assert not spool.has_data()

        # Non-video data is allowed to overshoot SPOOL_MAX_BYTES (it is small and precious).
        csv_file = make_file(tmp_path, "V3_data.csv", nbytes=1000)
        assert spool.spool_upload(CONTAINER, csv_file, api.StorageTier.HOT, move=True)
        assert len(spool.pending_uploads()) == 1

    @pytest.mark.unittest
    def test_part_files_cleaned_and_ignored(self, tmp_path: Path) -> None:
        root = tmp_path / "spool"
        stale = root / "upload" / CONTAINER / "HOT" / "V3_half_written.mp4.part"
        stale.parent.mkdir(parents=True)
        stale.write_bytes(b"x" * 10)

        spool = DiskSpool(root)
        assert not stale.exists(), "half-written files from a crashed run should be cleaned up at startup"
        assert not spool.has_data()


class TestAsyncCloudConnectorSpool:
    """Offline/spill behaviour of the AsyncCloudConnector, with all network methods patched out."""

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
    def test_offline_diverts_new_work_to_spool(self, tmp_path: Path) -> None:
        cc = self._make_cc(tmp_path)
        try:
            with cc._state_lock:
                cc._offline = True

            src = make_file(tmp_path, "V3_d01111111111_test.txt")
            cc.upload_to_container(CONTAINER, [src], delete_src=True)
            assert not src.exists()
            assert cc._upload_queue.empty(), "offline uploads must not be queued in memory"
            assert len(cc._spool.pending_uploads()) == 1

            csv_file = tmp_path / "V3_HEART_d01111111111_20260701.csv"
            csv_file.write_text("".join(CSV_DATA))
            assert cc.append_to_cloud(CONTAINER, csv_file, delete_src=True)
            assert not csv_file.exists()
            assert cc._upload_queue.empty()
            assert len(cc._spool.pending_appends()) == 1
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_memory_pressure_diverts_to_spool(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        cc = self._make_cc(tmp_path)
        try:
            monkeypatch.setattr(cc, "_memory_pressure", lambda: True)
            src = make_file(tmp_path, "V3_d01111111111_test.txt")
            cc.upload_to_container(CONTAINER, [src], delete_src=True)
            assert cc._upload_queue.empty()
            assert len(cc._spool.pending_uploads()) == 1
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
            cc.upload_to_container(CONTAINER, [src], delete_src=True)
            csv_file = tmp_path / "V3_HEART_d01111111111_20260701.csv"
            csv_file.write_text("".join(CSV_DATA))
            cc.append_to_cloud(CONTAINER, csv_file, delete_src=True)

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
    def test_drain_stops_on_failure_and_keeps_data(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cc = self._make_cc(tmp_path)
        try:
            self._stop_background_threads(cc)
            with cc._state_lock:
                cc._offline = True

            src = make_file(tmp_path, "V3_d01111111111_test.txt")
            cc.upload_to_container(CONTAINER, [src], delete_src=True)

            def failing_upload(self: CloudConnector, *args: object, **kwargs: object) -> None:
                raise ServiceRequestError(message="still offline")

            monkeypatch.setattr(CloudConnector, "upload_to_container", failing_upload)

            cc._drain_spool_once()

            assert cc.is_offline(), "a failed probe should leave the connector offline"
            assert len(cc._spool.pending_uploads()) == 1, "the spooled file must survive a failed drain"
        finally:
            cc.shutdown()

    @pytest.mark.unittest
    def test_shutdown_spills_queue_to_spool(self, tmp_path: Path) -> None:
        cc = self._make_cc(tmp_path)
        self._stop_background_threads(cc)
        with cc._state_lock:
            cc._offline = True  # Offline: shutdown must not attempt (or wait for) network I/O

        # Queue an upload (file staged in a tmp dir, as upload_to_container does for delete_src=True) and
        # an append, as if they were awaiting upload when the shutdown arrived.
        staged = make_file(tmp_path / "staging_tmp", "V3_d01111111111_test.txt")
        cc._upload_queue.put(AsyncUpload(CONTAINER, [staged], delete_src=True))
        cc._upload_queue.put(AsyncAppend(CONTAINER, "V3_HEART_d01111111111_20260701.csv", True, CSV_DATA))

        start = time.monotonic()
        cc.shutdown()
        elapsed = time.monotonic() - start

        assert elapsed < 10, f"offline shutdown must be fast and network-free (took {elapsed:.1f}s)"
        assert len(cc._spool.pending_uploads()) == 1
        assert len(cc._spool.pending_appends()) == 1
        assert not staged.exists(), "the staged file should have moved to the spool"
        assert not (tmp_path / "staging_tmp").exists(), "the empty staging dir should be cleaned up"

    @pytest.mark.unittest
    def test_startup_drains_spool_from_previous_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Simulate the post-reboot case: data is already in the spool when the connector starts.
        spool_root = tmp_path / "spool"
        seed = DiskSpool(spool_root)
        src = make_file(tmp_path, "V3_d01111111111_test.txt")
        assert seed.spool_upload(CONTAINER, src, api.StorageTier.HOT, move=True)

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
