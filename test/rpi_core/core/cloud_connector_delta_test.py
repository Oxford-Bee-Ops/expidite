"""Unit tests for CloudConnector's append-only delta/chunked download logic.

These exercise the private _download_blob_delta / _download_file methods directly against an in-memory
fake BlobClient, so they are fast and need no cloud connectivity. The key behaviour under test is that an
append-only download fetches [offset, size) as a sequence of single-GET chunks and therefore survives the
blob being appended to mid-download, without falling back to a full re-download.
"""

from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from azure.storage.blob import BlobClient

from expidite_rpi.core.cloud_connector import CloudConnector
from expidite_rpi.core.cloud_connector import cloud_connector as cc_module


class _FakeDownloadStream:
    def __init__(self, data: bytes) -> None:
        self._data = data

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

    def get_blob_properties(self) -> SimpleNamespace:
        self.properties_reads += 1
        return SimpleNamespace(size=len(self.content), etag="etag-0")

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
        return _FakeDownloadStream(data)

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
