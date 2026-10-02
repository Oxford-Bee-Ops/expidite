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
from unittest.mock import Mock

import pandas as pd
import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobType, ContainerClient

from expidite_rpi.core.cloud_connector import CloudConnector
from expidite_rpi.core.cloud_connector import cloud_connector as cc_module

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
    def __init__(self, data: bytes) -> None:
        self._data = data

    def readall(self) -> bytes:
        return self._data


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
        self.metadata: dict[str, str] = {}
        self.range_requests: list[tuple[int, int]] = []

    def exists(self) -> bool:
        return self.content is not None

    def get_blob_properties(self) -> SimpleNamespace:
        if self.content is None:
            raise ResourceNotFoundError(_NO_SUCH_BLOB)
        return SimpleNamespace(size=len(self.content), blob_type=self.blob_type, metadata=dict(self.metadata))

    def create_append_blob(self, metadata: dict[str, str] | None = None) -> None:
        if self.content is not None and self.blob_type != BlobType.APPENDBLOB:
            raise ResourceExistsError(_INVALID_BLOB_TYPE)
        self.creates += 1
        self.metadata = dict(metadata or {})
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

    def download_blob(self, offset: int = 0, length: int | None = None) -> _FakeDownloadStream:
        if self.content is None:
            raise ResourceNotFoundError(_NO_SUCH_BLOB)
        if length is None:
            return _FakeDownloadStream(self.content[offset:])
        self.range_requests.append((offset, length))
        return _FakeDownloadStream(self.content[offset : offset + length])

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
    cc._validated_append_files = {}
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


@pytest.mark.unittest
@pytest.mark.parametrize("state", ["missing", "existing", "cached"])
@pytest.mark.parametrize("lines", [[], [""], ["\n", "1,2\n"], [" \t\r\n", "1,2\n"]])
def test_blank_header_rejected_before_cloud_operations(
    state: str, lines: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = pd.DataFrame({"a": [1], "b": [2]})
    blob = _FakeBlobClient(DST_FILE, None if state == "missing" else _csv_bytes(frame), BlobType.APPENDBLOB)
    cc = _connector(blob)
    if state == "cached":
        assert _append(cc, frame)
    original_content = blob.content
    original_metadata = dict(blob.metadata)
    original_cache = dict(cc._validated_append_files)
    validate = Mock(side_effect=AssertionError("Invalid fragment must not contact cloud storage"))
    monkeypatch.setattr(cc, "_validate_container", validate)

    with pytest.raises(ValueError, match="non-blank CSV header"):
        cc._append_data_to_blob(DST_CONTAINER, DST_FILE, lines, swallow_exceptions=False)

    validate.assert_not_called()
    assert blob.content == original_content
    assert blob.metadata == original_metadata
    assert cc._validated_append_files == original_cache
    assert blob.creates == 0
    assert blob.deletes == 0


@pytest.mark.unittest
def test_blank_header_returns_false_when_exceptions_are_swallowed() -> None:
    blob = _FakeBlobClient(DST_FILE)
    assert _connector(blob)._append_data_to_blob(DST_CONTAINER, DST_FILE, ["\n", "1,2\n"]) is False
    assert blob.content is None


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
        assert (DST_CONTAINER, DST_FILE) in cc._validated_append_files

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


@pytest.mark.unittest
def test_old_spooled_header_does_not_skip_validation_of_new_columns() -> None:
    """Draining an old fragment must not validate a later fragment with added columns."""
    old = pd.DataFrame({"col_a": [1]})
    blob = _FakeBlobClient(DST_FILE, _csv_bytes(old), BlobType.APPENDBLOB)
    cc = _connector(blob)
    assert _append(cc, pd.DataFrame({"col_a": [2]}))
    assert blob.creates == 0

    new = pd.DataFrame({"col_a": [3], "journal_column_test": ["added"]})
    assert _append(cc, new, col_order=list(new.columns))

    assert blob.creates == 1
    merged = blob.as_csv()
    assert merged["col_a"].tolist() == [1, 2, 3]
    assert merged["journal_column_test"].isna().tolist() == [True, True, False]
    assert merged["journal_column_test"].iloc[-1] == "added"
    header_hash = cc_module._journal_header_hash(blob.lines()[0])
    assert blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] == header_hash
    assert cc._validated_append_files[DST_CONTAINER, DST_FILE] == tuple(merged.columns)

    assert _append(cc, new)
    assert blob.creates == 1
    assert len(blob.as_csv()) == 4


@pytest.mark.unittest
def test_journal_hash_tracks_actual_output_header_and_survives_appends() -> None:
    blob = _FakeBlobClient(DST_FILE)
    initial = pd.DataFrame({"col_a": [1]})
    assert _append(_connector(blob), initial)
    key = cc_module.JOURNAL_HEADER_METADATA_KEY
    first_hash = blob.metadata[key]
    assert first_hash == cc_module._journal_header_hash(blob.lines()[0])
    assert _append(_connector(blob), initial)
    assert blob.metadata[key] == first_hash
    # Incoming order differs from output order; the hash must describe the merged CSV, not the fragment.
    assert _append(_connector(blob), pd.DataFrame({"col_c": [3], "col_a": [2]}), col_order=["col_a", "col_c"])
    assert blob.lines()[0] == "col_a,col_c"
    assert blob.metadata[key] == cc_module._journal_header_hash("col_a,col_c")
    assert blob.metadata[key] != first_hash
    assert blob.deletes == 0


@pytest.mark.unittest
def test_legacy_append_does_not_add_hash_or_rewrite() -> None:
    frame = pd.DataFrame({"col1": [1]})
    blob = _FakeBlobClient(DST_FILE, _csv_bytes(frame), BlobType.APPENDBLOB)
    assert _append(_connector(blob), frame)
    assert blob.metadata == {}
    assert blob.creates == 0


class TestHeadersMatch:
    @pytest.mark.unittest
    def test_long_legacy_header_is_read_in_full_and_matches(self) -> None:
        """A header longer than one read must not be truncated into a false mismatch and a rewrite."""
        frame = pd.DataFrame({f"column_{i:04d}": [i] for i in range(600)})
        assert len(_csv_lines(frame)[0]) > 4096
        blob = _FakeBlobClient(DST_FILE, _csv_bytes(frame), BlobType.APPENDBLOB)

        assert _append(_connector(blob), frame)

        assert blob.creates == 0, "matching headers must not rewrite the blob"
        assert len(blob.range_requests) > 1
        assert blob.lines()[1:] == blob.lines()[1:2] * 2

    @pytest.mark.unittest
    def test_matching_header_metadata_skips_download(self) -> None:
        frame = pd.DataFrame({"col1": [1], "col2": [2]})
        blob = _FakeBlobClient(DST_FILE, _csv_bytes(frame), BlobType.APPENDBLOB)
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("col1,col2")

        assert _append(_connector(blob), frame)

        assert blob.range_requests == []
        assert blob.creates == 0
        assert blob.lines() == ["col1,col2", "1,2", "1,2"]

    @pytest.mark.unittest
    def test_mismatched_header_metadata_merges(self) -> None:
        blob = _FakeBlobClient(
            DST_FILE, _csv_bytes(pd.DataFrame({"col1": [1], "col2": [2]})), BlobType.APPENDBLOB
        )
        blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("col1,col2")

        assert _append(_connector(blob), pd.DataFrame({"col1": [3], "col3": [4]}), col_order=["col1", "col3"])

        assert blob.creates == 1
        assert blob.lines()[0] == "col1,col3,col2"
        assert blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] == cc_module._journal_header_hash(
            "col1,col3,col2"
        )
        assert blob.as_csv()["col2"].iloc[0] == 2


@pytest.mark.unittest
@pytest.mark.parametrize("restart_before_old", [True, False])
def test_new_old_new_fragments_preserve_columns_and_values(restart_before_old: bool) -> None:
    """Old fragments are padded/reordered without rewriting or dropping newer values."""
    blob = _FakeBlobClient(DST_FILE)
    cc = _connector(blob)
    first = pd.DataFrame({"col_a": [1], "col_c": ["new value"], "col_b": ["first"]})
    assert _append(cc, first, col_order=list(first.columns))
    key = cc_module.JOURNAL_HEADER_METADATA_KEY
    header_hash = blob.metadata[key]
    if restart_before_old:
        cc = _connector(blob)
    for value in (2, 3):
        old = pd.DataFrame({"col_b": ['old, "quoted"\nvalue'], "col_a": [value]})
        assert _append(cc, old, col_order=list(old.columns))
    last = pd.DataFrame({"col_a": [4], "col_b": ["last"], "col_c": ["another new value"]})
    assert _append(cc, last, col_order=list(last.columns))
    result = blob.as_csv()
    assert list(result.columns) == ["col_a", "col_c", "col_b"]
    assert result["col_a"].tolist() == [1, 2, 3, 4]
    assert result["col_b"].tolist() == ["first", 'old, "quoted"\nvalue', 'old, "quoted"\nvalue', "last"]
    assert result["col_c"].iloc[0] == "new value"
    assert result["col_c"].iloc[-1] == "another new value"
    assert result["col_c"].isna().tolist() == [False, True, True, False]
    assert blob.creates == 1
    assert blob.metadata[key] == header_hash


@pytest.mark.unittest
def test_empty_blob_with_header_metadata_gets_header_row() -> None:
    """create_append_blob() succeeded but append_block() failed: the retry must still write the header."""
    blob = _FakeBlobClient(DST_FILE, b"", BlobType.APPENDBLOB)
    blob.metadata[cc_module.JOURNAL_HEADER_METADATA_KEY] = cc_module._journal_header_hash("col1,col2")

    assert _append(_connector(blob), pd.DataFrame({"col1": [1], "col2": [2]}))

    assert blob.lines() == ["col1,col2", "1,2"]
