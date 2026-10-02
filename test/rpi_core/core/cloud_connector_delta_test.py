"""Unit tests for CloudConnector's append-only delta/chunked download logic.

These exercise the private _download_blob_delta / _download_file methods directly against an in-memory
fake BlobClient, so they are fast and need no cloud connectivity. The key behaviour under test is that an
append-only download fetches [offset, size) as a sequence of single-GET chunks and therefore survives the
blob being appended to mid-download, without falling back to a full re-download.
"""

from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import ResourceModifiedError
from azure.storage.blob import BlobClient, ContainerClient

from expidite_rpi.core.cloud_connector import CloudConnector
from expidite_rpi.core.cloud_connector import cloud_connector as cc_module


class _FakeDownloadStream:
    def __init__(self, data: bytes, metadata: dict[str, str] | None = None) -> None:
        self._data = data
        self.properties = SimpleNamespace(metadata=dict(metadata or {}))

    def readall(self) -> bytes:
        return self._data


class _FakeBlobClient:
    """In-memory stand-in for an Azure BlobClient.

    Records every range request and can simulate an append-only writer growing the blob concurrently with
    our reads (``append_each`` is appended to the blob after every ``download_blob`` call). Because appends
    only ever add bytes at the end, any range below the size at request time stays stable - exactly the
    append-only guarantee the delta download relies on.
    """

    def __init__(self, content: bytes, append_each: bytes = b"") -> None:
        self.content = content
        self.append_each = append_each
        self.range_requests: list[tuple[int, int]] = []
        self.full_downloads = 0
        self.properties_reads = 0
        self.metadata: dict[str, str] = {}

    def get_blob_properties(self) -> SimpleNamespace:
        self.properties_reads += 1
        return SimpleNamespace(size=len(self.content), etag="etag-0", metadata=dict(self.metadata))

    def download_blob(self, offset: int = 0, length: int | None = None) -> _FakeDownloadStream:
        if length is None:
            # Open-ended read used by the full-download path (_download_blob).
            self.full_downloads += 1
            data = self.content[offset:]
        else:
            self.range_requests.append((offset, length))
            data = self.content[offset : offset + length]
        if self.append_each:
            self.content += self.append_each
        return _FakeDownloadStream(data, self.metadata)

    @property
    def as_client(self) -> BlobClient:
        """Cast to BlobClient for passing into the methods under test (the duck-typed surface matches)."""
        return cast(BlobClient, self)


def _connector() -> CloudConnector:
    # Bypass __init__ (which requires cloud credentials); the methods under test only use self to dispatch.
    return CloudConnector.__new__(CloudConnector)


class TestDownloadBlobDelta:
    @pytest.mark.unittest
    def test_small_delta_is_a_single_request(self, tmp_path: Path) -> None:
        """A delta smaller than the chunk size is fetched in one ranged GET starting at the offset."""
        content = b"A" * 100
        blob = _FakeBlobClient(content)
        dst = tmp_path / "f.bin"
        dst.write_bytes(content[:90])  # existing partial local copy

        _connector()._download_blob_delta(blob.as_client, dst, offset=90)

        assert dst.read_bytes() == content
        assert blob.range_requests == [(90, 10)]
        assert blob.full_downloads == 0

    @pytest.mark.unittest
    def test_survives_concurrent_append(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """An append on every chunk read must not abort the download or trigger a full re-download.

        With a small chunk size the delta spans several requests; the old code would have hit
        ResourceModifiedError and re-downloaded the whole file from byte 0.
        """
        monkeypatch.setattr(cc_module, "_DELTA_CHUNK_BYTES", 16)
        original = b"A" * 100
        blob = _FakeBlobClient(original, append_each=b"Z" * 10)
        dst = tmp_path / "f.bin"
        dst.write_bytes(original[:40])

        _connector()._download_blob_delta(blob.as_client, dst, offset=40)

        # We download exactly the snapshot tail [40, 100); concurrent appends are picked up on a later poll.
        assert dst.read_bytes() == original
        assert blob.full_downloads == 0
        # Never re-read from the start, and every chunk respects the configured size.
        assert blob.range_requests[0][0] == 40
        assert all(off >= 40 for off, _ in blob.range_requests)
        assert all(length <= 16 for _, length in blob.range_requests)
        # Chunks are contiguous and cover the whole tail.
        cursor = 40
        for off, length in blob.range_requests:
            assert off == cursor
            cursor += length
        assert cursor == 100

    @pytest.mark.unittest
    def test_large_delta_is_chunked(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A delta larger than the chunk size is split into multiple single-GET requests."""
        monkeypatch.setattr(cc_module, "_DELTA_CHUNK_BYTES", 10)
        content = bytes(range(256)) * 4  # 1024 deterministic bytes
        blob = _FakeBlobClient(content)
        dst = tmp_path / "f.bin"
        dst.write_bytes(content[:1000])

        _connector()._download_blob_delta(blob.as_client, dst, offset=1000)

        assert dst.read_bytes() == content
        assert len(blob.range_requests) == 3  # 24 bytes / 10 -> 10 + 10 + 4
        assert [length for _, length in blob.range_requests] == [10, 10, 4]

    @pytest.mark.unittest
    def test_nothing_new_makes_no_request(self, tmp_path: Path) -> None:
        """If the local copy already matches the blob size, nothing is downloaded."""
        content = b"ABC"
        blob = _FakeBlobClient(content)
        dst = tmp_path / "f.bin"
        dst.write_bytes(content)

        _connector()._download_blob_delta(blob.as_client, dst, offset=3)

        assert dst.read_bytes() == content
        assert blob.range_requests == []
        assert blob.full_downloads == 0

    @pytest.mark.unittest
    def test_truncated_blob_redownloads_from_scratch(self, tmp_path: Path) -> None:
        """If the blob is smaller than our local copy it was rewritten/truncated -> full re-download."""
        blob = _FakeBlobClient(b"BBB")
        dst = tmp_path / "f.bin"
        dst.write_bytes(b"A" * 100)  # stale, larger local copy

        _connector()._download_blob_delta(blob.as_client, dst, offset=100)

        assert dst.read_bytes() == b"BBB"
        assert blob.range_requests[0][0] == 0  # re-read from the start
        assert blob.full_downloads == 0  # still via the chunked path, not _download_blob

    @pytest.mark.unittest
    def test_cold_start_offset_zero(self, tmp_path: Path) -> None:
        """offset=0 downloads the whole blob via the chunked path, creating the file."""
        content = b"A" * 50
        blob = _FakeBlobClient(content)
        dst = tmp_path / "new.bin"  # does not exist yet

        _connector()._download_blob_delta(blob.as_client, dst, offset=0)

        assert dst.read_bytes() == content
        assert blob.range_requests[0][0] == 0


class TestDownloadFileRouting:
    @pytest.mark.unittest
    def test_append_only_cold_start_uses_chunked_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An append-only from-scratch download (offset=0) goes via the chunked, append-safe path."""
        monkeypatch.setattr(cc_module, "_DELTA_CHUNK_BYTES", 16)
        original = b"A" * 50
        blob = _FakeBlobClient(original, append_each=b"Z" * 5)
        dst = tmp_path / "sub" / "f.bin"  # parent dir does not exist yet

        _connector()._download_file(blob.as_client, dst, offset=0, append_only=True)

        assert dst.read_bytes() == original  # snapshot tail only, survives concurrent appends
        assert blob.full_downloads == 0
        assert blob.range_requests[0][0] == 0

    @pytest.mark.unittest
    def test_general_cold_start_uses_full_download(self, tmp_path: Path) -> None:
        """A general (non append-only) download keeps using the full-blob path with its If-Match guard."""
        content = b"hello world"
        blob = _FakeBlobClient(content)
        dst = tmp_path / "f.bin"

        _connector()._download_file(blob.as_client, dst, offset=0, append_only=False)

        assert dst.read_bytes() == content
        assert blob.full_downloads == 1
        assert blob.range_requests == []


class TestJournalHeaders:
    @pytest.mark.unittest
    @pytest.mark.parametrize("with_hash", [True, False])
    def test_matching_or_legacy_header_keeps_delta_without_baseline(
        self, tmp_path: Path, with_hash: bool
    ) -> None:
        content = b"a,b\n1,2\n3,4\n"
        blob = _FakeBlobClient(content)
        if with_hash:
            blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(content[:8])
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == content
        assert blob.range_requests == [(8, 4)]
        assert blob.properties_reads == 1
        assert list(tmp_path.iterdir()) == [dst]

    @pytest.mark.unittest
    @pytest.mark.parametrize("header", ["a,b,c", "b,a", "a,c"])
    def test_added_reordered_or_renamed_columns_replace_local_file(self, tmp_path: Path, header: str) -> None:
        content = (header + "\n1,2,3\n").encode()
        blob = _FakeBlobClient(content)
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash(header)
        dst = tmp_path / "journal.csv"
        dst.write_bytes(b"a,b\n1,2\n")
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == content
        assert blob.range_requests == [(0, len(content))]
        assert blob.properties_reads == 1

    @pytest.mark.unittest
    def test_same_size_schema_rewrite_is_replaced(self, tmp_path: Path) -> None:
        blob = _FakeBlobClient(b"a,c\n1,2\n")
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,c")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(b"a,b\n1,2\n")
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == blob.content
        assert blob.range_requests == [(0, 8)]

    @pytest.mark.unittest
    def test_quoting_and_line_endings_do_not_force_replacement(self, tmp_path: Path) -> None:
        local = b'"a","b"\r\n1,2\r\n'
        blob = _FakeBlobClient(local + b"3,4\r\n")
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b\n")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(local)
        _connector()._download_file(blob.as_client, dst, offset=len(local), check_journal_headers=True)
        assert blob.range_requests == [(len(local), 5)]
        assert dst.read_bytes() == blob.content

    @pytest.mark.unittest
    def test_concurrent_appends_keep_using_delta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cc_module, "_DELTA_CHUNK_BYTES", 4)
        content = b"a,b\n1,2\n3,4\n5,6\n"
        blob = _FakeBlobClient(content, append_each=b"7,8\n")
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(content[:8])
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == content
        assert blob.range_requests == [(8, 4), (12, 4)]
        assert blob.properties_reads == 1

    @pytest.mark.unittest
    def test_schema_change_mid_download_retries_in_full(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cc_module, "_DELTA_CHUNK_BYTES", 4)
        key = cc_module.JOURNAL_HEADER_METADATA_KEY
        blob = _FakeBlobClient(b"a,b\n1,2\n3,4\n5,6\n")
        blob.metadata[key] = cc_module._journal_header_hash("a,b")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(blob.content[:8])
        original = blob.download_blob

        def rewrite(offset: int = 0, length: int | None = None) -> _FakeDownloadStream:
            if offset >= 12:
                blob.content = b"a,b,c\n1,2,3\n4,5,6\n"
                blob.metadata[key] = cc_module._journal_header_hash("a,b,c")
            return original(offset, length)

        monkeypatch.setattr(blob, "download_blob", rewrite)
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == blob.content
        assert blob.range_requests[:3] == [(8, 4), (12, 4), (0, 4)]
        assert blob.properties_reads == 2

    @pytest.mark.unittest
    def test_repeated_schema_changes_give_up_after_three_attempts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cc_module, "_DELTA_CHUNK_BYTES", 4)
        key = cc_module.JOURNAL_HEADER_METADATA_KEY
        blob = _FakeBlobClient(b"a,b\n1,2\n3,4\n5,6\n")
        dst = tmp_path / "journal.csv"
        original = blob.download_blob

        def always_rewrite(offset: int = 0, length: int | None = None) -> _FakeDownloadStream:
            blob.metadata[key] = cc_module._journal_header_hash(f"col{len(blob.range_requests)}")
            return original(offset, length)

        monkeypatch.setattr(blob, "download_blob", always_rewrite)
        with pytest.raises(ResourceModifiedError, match="Journal header changed"):
            _connector()._download_file(blob.as_client, dst, append_only=True, check_journal_headers=True)
        assert blob.properties_reads == 3
        assert not dst.exists(), "an exhausted download must not leave a mixed-schema delta baseline"

    @pytest.mark.unittest
    @pytest.mark.parametrize("with_hash", [True, False])
    def test_missing_local_file_with_stale_offset_downloads_in_full(
        self, tmp_path: Path, with_hash: bool
    ) -> None:
        blob = _FakeBlobClient(b"a,b\n1,2\n3,4\n")
        if with_hash:
            blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b")
        dst = tmp_path / "missing.csv"
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == blob.content
        assert blob.range_requests == [(0, len(blob.content))]

    @pytest.mark.unittest
    def test_local_file_deleted_during_header_check_downloads_in_full(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        blob = _FakeBlobClient(b"a,b\n1,2\n3,4\n")
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(blob.content[:8])
        original = Path.open

        def delete_before_read(
            path: Path,
            mode: str = "r",
            buffering: int = -1,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
        ) -> object:
            if path == dst and encoding == "utf-8":
                path.unlink()
            return original(path, mode, buffering, encoding, errors, newline)

        monkeypatch.setattr(Path, "open", delete_before_read)
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == blob.content

    @pytest.mark.unittest
    @pytest.mark.parametrize("batch_size", [1, 10_000])
    def test_batch_finishes_other_journals_before_reporting_schema_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, batch_size: int
    ) -> None:
        monkeypatch.setattr(cc_module, "_DELTA_DOWNLOAD_BATCH_SIZE", batch_size)
        bad = _FakeBlobClient(b"a,b\n1,2\n")
        good = _FakeBlobClient(b"a,b\n3,4\n")
        original = bad.download_blob

        def always_rewrite(offset: int = 0, length: int | None = None) -> _FakeDownloadStream:
            bad.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = str(len(bad.range_requests))
            return original(offset, length)

        monkeypatch.setattr(bad, "download_blob", always_rewrite)
        container = MagicMock()
        container.get_blob_client.side_effect = lambda name: (
            {"bad.csv": bad, "good.csv": good}[name].as_client
        )
        cc = _connector()
        monkeypatch.setattr(cc, "_validate_container", lambda _name: cast("ContainerClient", container))
        with pytest.raises(ResourceModifiedError, match="Journal header changed") as caught:
            cc.download_container_deltas(
                "journals", tmp_path, {"bad.csv": 0, "good.csv": 0}, check_journal_headers=True
            )
        assert (tmp_path / "good.csv").read_bytes() == good.content
        assert not (tmp_path / "bad.csv").exists()
        assert "Journal download failed: bad.csv" in caught.value.__notes__
        assert "Failed to download 1 journal(s)" in caught.value.__notes__

    @pytest.mark.unittest
    def test_batch_reports_multiple_failures_without_changing_exception_type(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cc = _connector()
        container = MagicMock()
        monkeypatch.setattr(cc, "_validate_container", lambda _name: cast("ContainerClient", container))

        def fail_download(
            blob: BlobClient, dst: Path, offset: int, *, append_only: bool, check_journal_headers: bool
        ) -> str:
            raise ResourceModifiedError(dst.name)

        monkeypatch.setattr(cc, "_download_file", fail_download)
        with pytest.raises(ResourceModifiedError) as caught:
            cc.download_container_deltas("journals", tmp_path, {"one.csv": 0, "two.csv": 0})
        assert "Failed to download 2 journal(s)" in caught.value.__notes__
        remaining = caught.value.__cause__
        assert isinstance(remaining, ExceptionGroup)
        assert len(remaining.exceptions) == 1
        names = {str(caught.value), str(remaining.exceptions[0])}
        assert names == {"one.csv", "two.csv"}

    @pytest.mark.unittest
    def test_undecodable_local_header_forces_full_download(self, tmp_path: Path) -> None:
        blob = _FakeBlobClient(b"a,b\n1,2\n3,4\n")
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(b"\xff\xfe\n1,2\n")
        _connector()._download_file(blob.as_client, dst, offset=8, check_journal_headers=True)
        assert dst.read_bytes() == blob.content
        assert blob.range_requests == [(0, len(blob.content))]

    @pytest.mark.unittest
    @pytest.mark.parametrize("with_hash", [True, False])
    def test_cold_start_downloads_without_sidecars(self, tmp_path: Path, with_hash: bool) -> None:
        blob = _FakeBlobClient(b"a,b\n1,2\n")
        if with_hash:
            blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b")
        dst = tmp_path / "journal.csv"
        _connector()._download_file(blob.as_client, dst, check_journal_headers=True)
        assert dst.read_bytes() == blob.content
        assert list(tmp_path.iterdir()) == [dst]

    @pytest.mark.unittest
    def test_container_download_enables_header_check(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        blob = _FakeBlobClient(b"a,b,c\n1,2,3\n")
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("a,b,c")
        dst = tmp_path / "journal.csv"
        dst.write_bytes(b"a,b\n1,2\n")
        container = MagicMock()
        container.get_blob_client.return_value = blob.as_client
        cc = _connector()
        monkeypatch.setattr(cc, "_validate_container", lambda _name: cast("ContainerClient", container))
        cc.download_container_deltas("journals", tmp_path, {dst.name: 8}, check_journal_headers=True)
        assert dst.read_bytes() == blob.content
        assert blob.range_requests == [(0, len(blob.content))]
