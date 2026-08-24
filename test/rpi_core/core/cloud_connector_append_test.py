"""Unit tests for CloudConnector._append_data_to_blob's header-mismatch merge path.

These run against an in-memory fake BlobClient that reproduces Azure's blob-type rule - Put Blob
overwrites an existing blob of the same type but rejects one that would change the type - so they are
fast and need no cloud connectivity. The behaviour under test is that a header change re-creates the
remote file as an append blob whatever type it started as, which previously only worked if the existing
blob happened to be zero length.
"""

import io
from threading import Lock
from types import SimpleNamespace
from typing import cast

import pandas as pd
import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobType, ContainerClient

from expidite_rpi.core.cloud_connector import CloudConnector

DST_CONTAINER = "expidite-upload"
DST_FILE = "journal.csv"

# The messages Azure returns for the two conditions the fake reproduces.
_NO_SUCH_BLOB = "The specified blob does not exist."
_INVALID_BLOB_TYPE = "The blob type is invalid for this operation."


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    """Render a DataFrame as the bytes an existing cloud file would hold."""
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _csv_lines(frame: pd.DataFrame) -> list[str]:
    """Render a DataFrame the way a local journal file is read: a list of lines, headers first."""
    return _csv_bytes(frame).decode("utf-8").splitlines(keepends=True)


class _FakeDownloadStream:
    def __init__(self, data: bytes, encoding: str | None) -> None:
        self._data = data
        self._encoding = encoding

    def readall(self) -> bytes:
        return self._data

    def read(self, chars: int | None = None) -> str:
        assert self._encoding is not None, "read(chars=...) is only used on a text download"
        text = self._data.decode(self._encoding)
        return text if chars is None else text[:chars]


class _FakeBlobClient:
    """In-memory stand-in for an Azure BlobClient that honours Azure's blob-type rules.

    content is None when the blob does not exist. create_append_blob() maps to Put Blob: it overwrites an
    existing append blob in place, but Azure answers 409 InvalidBlobType if the existing blob is of another
    type, so we do too - that is the case the merge path has to delete out of the way first.
    """

    def __init__(self, name: str, content: bytes | None = None, blob_type: BlobType | None = None) -> None:
        self.blob_name = name
        self.content = content
        self.blob_type = blob_type
        self.deletes = 0
        self.creates = 0

    def exists(self) -> bool:
        return self.content is not None

    def get_blob_properties(self) -> SimpleNamespace:
        if self.content is None:
            raise ResourceNotFoundError(_NO_SUCH_BLOB)
        return SimpleNamespace(size=len(self.content), blob_type=self.blob_type)

    def create_append_blob(self) -> None:
        if self.content is not None and self.blob_type != BlobType.APPENDBLOB:
            raise ResourceExistsError(_INVALID_BLOB_TYPE)
        self.creates += 1
        self.content = b""
        self.blob_type = BlobType.APPENDBLOB

    def append_block(self, data: bytes) -> None:
        if self.content is None or self.blob_type != BlobType.APPENDBLOB:
            raise ResourceExistsError(_INVALID_BLOB_TYPE)
        self.content += data

    def delete_blob(self) -> None:
        if self.content is None:
            raise ResourceNotFoundError(_NO_SUCH_BLOB)
        self.deletes += 1
        self.content = None
        self.blob_type = None

    def download_blob(self, encoding: str | None = None) -> _FakeDownloadStream:
        if self.content is None:
            raise ResourceNotFoundError(_NO_SUCH_BLOB)
        return _FakeDownloadStream(self.content, encoding)

    def as_csv(self) -> pd.DataFrame:
        assert self.content is not None, "blob does not exist"
        return pd.read_csv(io.BytesIO(self.content))

    def lines(self) -> list[str]:
        assert self.content is not None, "blob does not exist"
        return self.content.decode("utf-8").splitlines()


class _FakeContainerClient:
    def __init__(self, blob: _FakeBlobClient) -> None:
        self.blob = blob

    def get_blob_client(self, name: str) -> _FakeBlobClient:
        assert name == self.blob.blob_name, f"unexpected blob {name}"
        return self.blob


def _connector(blob: _FakeBlobClient) -> CloudConnector:
    """Bypass __init__ (which requires cloud credentials) and pre-seed the container cache with our fake."""
    cc = CloudConnector.__new__(CloudConnector)
    cc._validated_containers = {DST_CONTAINER: cast(ContainerClient, _FakeContainerClient(blob))}
    cc._append_locks = {}
    cc._append_locks_lock = Lock()
    cc._validated_append_files = set()
    return cc


def _append(cc: CloudConnector, local: pd.DataFrame, col_order: list[str] | None = None) -> bool:
    # swallow_exceptions=False so a failure surfaces as the underlying Azure error rather than a bare False.
    return cc._append_data_to_blob(
        dst_container=DST_CONTAINER,
        dst_file=DST_FILE,
        lines_to_append=_csv_lines(local),
        col_order=col_order,
        swallow_exceptions=False,
    )


class TestAppendDataToBlobHeaderMismatch:
    @pytest.mark.unittest
    def test_recreates_non_empty_block_blob_as_append_blob(self) -> None:
        """A header change over a *non-empty* block blob must delete it and re-create it as an append blob.

        This is what upload_to_container leaves behind. The old guard only deleted zero-length blobs, so
        create_append_blob() hit InvalidBlobType here and the journal could never be written again.
        """
        blob = _FakeBlobClient(
            DST_FILE, _csv_bytes(pd.DataFrame({"col_a": [1, 2], "col_b": [3, 4]})), BlobType.BLOCKBLOB
        )

        result = _append(_connector(blob), pd.DataFrame({"col_b": [5, 6], "col_c": [7, 8]}))

        assert result is True
        assert blob.deletes == 1, "the block blob must be deleted before the append blob is created"
        assert blob.blob_type == BlobType.APPENDBLOB
        merged = blob.as_csv()
        assert list(merged.columns) == ["col_a", "col_b", "col_c"]
        assert len(merged) == 4, "both the pre-existing rows and the new rows must survive the merge"
        assert merged["col_a"].tolist()[:2] == [1, 2]
        assert merged["col_c"].tolist()[2:] == [7, 8]

    @pytest.mark.unittest
    def test_recreates_zero_byte_block_blob_as_append_blob(self) -> None:
        """The zero-length block blob case must keep working - it is the one the old guard did handle."""
        blob = _FakeBlobClient(DST_FILE, b"", BlobType.BLOCKBLOB)

        result = _append(_connector(blob), pd.DataFrame({"col1": [10, 20], "col2": [30, 40]}))

        assert result is True
        assert blob.deletes == 1
        assert blob.blob_type == BlobType.APPENDBLOB
        written = blob.as_csv()
        assert list(written.columns) == ["col1", "col2"]
        assert len(written) == 2

    @pytest.mark.unittest
    def test_overwrites_existing_append_blob_without_deleting_it(self) -> None:
        """An append blob is overwritten in place: no delete, so no gap where the blob doesn't exist."""
        blob = _FakeBlobClient(
            DST_FILE, _csv_bytes(pd.DataFrame({"col_a": [1, 2], "col_b": [3, 4]})), BlobType.APPENDBLOB
        )

        result = _append(_connector(blob), pd.DataFrame({"col_b": [5, 6], "col_c": [7, 8]}))

        assert result is True
        assert blob.deletes == 0, "an append blob is replaced by create_append_blob() alone"
        merged = blob.as_csv()
        assert list(merged.columns) == ["col_a", "col_b", "col_c"]
        assert len(merged) == 4

    @pytest.mark.unittest
    def test_col_order_is_honoured_when_recreating(self) -> None:
        """The re-created file uses the caller's column order, so later header-less appends line up."""
        blob = _FakeBlobClient(
            DST_FILE, _csv_bytes(pd.DataFrame({"col_a": [1], "col_b": [2]})), BlobType.BLOCKBLOB
        )

        result = _append(
            _connector(blob),
            pd.DataFrame({"col_c": [9], "col_b": [8]}),
            col_order=["col_a", "col_b", "col_c"],
        )

        assert result is True
        assert blob.lines()[0] == "col_a,col_b,col_c"


class TestAppendDataToBlobOtherPaths:
    @pytest.mark.unittest
    def test_creates_blob_with_headers_when_missing(self) -> None:
        """A file that does not exist yet is created as an append blob and keeps its header row."""
        blob = _FakeBlobClient(DST_FILE)
        cc = _connector(blob)

        result = _append(cc, pd.DataFrame({"col1": [1], "col2": [2]}))

        assert result is True
        assert blob.deletes == 0
        assert blob.lines() == ["col1,col2", "1,2"]
        assert DST_FILE in cc._validated_append_files

    @pytest.mark.unittest
    def test_matching_headers_append_without_repeating_the_header_row(self) -> None:
        """When the headers already match we append the data rows only."""
        blob = _FakeBlobClient(
            DST_FILE, _csv_bytes(pd.DataFrame({"col1": [1], "col2": [2]})), BlobType.APPENDBLOB
        )

        result = _append(_connector(blob), pd.DataFrame({"col1": [3], "col2": [4]}))

        assert result is True
        assert blob.deletes == 0
        assert blob.creates == 0, "an in-place append must not re-create the blob"
        assert blob.lines() == ["col1,col2", "1,2", "3,4"]
