from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from threading import RLock
from typing import Optional

import pandas as pd

from sensor_core import api, file_naming
from sensor_core import configuration as root_cfg
from sensor_core.configuration import Mode
from sensor_core.dp_config_objects import Stream
from sensor_core.utils.cloud_journal import CloudJournal
from sensor_core.utils.journal import Journal

logger = root_cfg.setup_logger("sensor_core")


class JournalPool(ABC):
    """The JournalPool is responsible for thread-safe saving of data to journal files and onward to archive.

    The JournalPool is a singleton shared by all DPtreeNode instances and should be retrieved using 
    JournalPool.get().

    The internal implementation of the JournalPool is different for EDGE and ETL modes.
    On the EDGE, there is a "Journal" in the JournalPool per DPtreeNode type_id.
    On the ETL, we use "CloudJournal" objects to manage the data, with 1 Journal per DPtreeNode type_id 
    per day.
    In both cases, data is stored based on its bapi.RECORD_ID.DS_TYPE_ID and bapi.RECORD_ID.TIMESTAMP in the 
    appropriate CJ.
    """

    _instance: Optional[JournalPool] = None

    @staticmethod
    def get(mode: Mode) -> JournalPool:
        """Get the singleton instance of the JournalPool"""
        if JournalPool._instance is None:
            if mode is None:
                raise ValueError("DPtreeNode mode has not been set.")
            # We create different concrete implementations depending on the mode we're running in
            if mode == Mode.EDGE:
                JournalPool._instance = CloudJournalPool() # @@@ LocalJournalPool()
            else:
                JournalPool._instance = CloudJournalPool()
        return JournalPool._instance

    @abstractmethod
    def add_rows(self, stream: Stream, 
                 data: list[dict], 
                 timestamp: Optional[datetime] = None) -> None:
        """Add data rows as a list of dictionaries

        The fields in each dictionary must match the DPtreeNodeCfg reqd_fields."""

        assert False, "Abstract method needs to be implemented"

    @abstractmethod
    def add_rows_from_df(self, 
                         stream: Stream, 
                         data: pd.DataFrame, 
                         timestamp: Optional[datetime] = None
    ) -> None:
        """Add data in the form of a Pandas DataFrame to the Journal, which will auto-sync to the cloud

        All data MUST relate to the same DAY as timestamp."""

        assert False, "Abstract method needs to be implemented"

    @abstractmethod
    def flush_journals(self) -> None:
        """Flush all journals to disk and onwards to archive"""

        assert False, "Abstract method needs to be implemented"

    @abstractmethod
    def stop(self) -> None:
        """Stop the JournalPool, flush all data and exit any threads"""

        assert False, "Abstract method needs to be implemented"

class CloudJournalPool(JournalPool):
    """The CloudJournalPool is a concrete implementation of a JournalPool for running in ETL mode.

    It is based on a pool of CloudJournal instances.
    """

    def __init__(self) -> None:
        self._cj_pool: dict[str, CloudJournal] = {}
        self.jlock = RLock()

    def add_rows(self, 
                 stream: Stream, 
                 data: list[dict], 
                 timestamp: Optional[datetime] = None) -> None:
        """Add data to the appropriate CloudJournal, which will auto-sync to the cloud

        All data MUST relate to the same DAY as timestamp."""

        assert timestamp is not None, "Timestamp must be provided for add_rows_from_df with CloudJournalPool"
        with self.jlock:
            cj = self._get_journal(stream, timestamp)
            cj.add_rows(data)

    def add_rows_from_df(
        self, stream: Stream, data: pd.DataFrame, timestamp: Optional[datetime] = None
    ) -> None:
        """Add data to the appropriate CloudJournal, which will auto-sync to the cloud

        All data MUST relate to the same DAY as timestamp."""
        if timestamp is None:
            timestamp = api.utc_now()

        with self.jlock:
            cj = self._get_journal(stream, timestamp)
            cj.add_rows_from_df(data)

    def flush_journals(self) -> None:
        """Flush all journals to disk and onwards to archive"""
        with self.jlock:
            # We can call flush_all on any CloudJournal in the pool and all will get flushed
            for cj in self._cj_pool.values():
                cj.flush_all()
                break

    def stop(self) -> None:
        """Stop the CloudJournalPool, flush all data and exit any threads"""
        with self.jlock:
            # We can call stop on any CloudJournal in the pool and all will get stopped
            for cj in self._cj_pool.values():
                cj.stop()
                break

    def _get_journal(self, stream: Stream, day: datetime) -> CloudJournal:
        """Generate the CloudJournal filename for a DPtreeNodeCfg.

        The V3 filename format is:
            V3_{DPtreeNodeCfg_type_id}_{day}.csv
        """
        # Check that the output_fields contain at least all the bapi.REQD_RECORD_ID_FIELDS
        assert stream.fields is not None, (
            f"output_fields must be set in {stream}")

        fname = file_naming.get_cloud_journal_filename(stream.type_id, day)

        if fname.name not in self._cj_pool:
            # Users can choose a cloud_container per DS or use the default one
            cloud_container = stream.cloud_container
            if cloud_container is None:
                cloud_container = root_cfg.my_device.cc_for_journals
            cj = CloudJournal(fname, cloud_container, [*api.ALL_RECORD_ID_FIELDS, *stream.fields])
            self._cj_pool[fname.name] = cj
        else:
            cj = self._cj_pool[fname.name]
        return cj


class LocalJournalPool(JournalPool):
    """The LocalJournalPool is a concrete implementation of a JournalPool for running in EDGE mode.

    It is based on a pool of Journal instances.
    """

    def __init__(self) -> None:
        self._jpool: dict[str, Journal] = {}
        self.jlock = RLock()

    def add_rows(self, 
                 stream: Stream, 
                 data: list[dict], 
                 timestamp: Optional[datetime] = None) -> None:
        """Add data to the appropriate Journal, which will auto-upload to the cloud"""

        with self.jlock:
            j = self._get_journal(stream)
            j.add_rows(data)

    def add_rows_from_df(self, 
                         stream: Stream, 
                         data: pd.DataFrame, 
                         timestamp: Optional[datetime] = None
    ) -> None:
        """Add data to the appropriate Journal, which will auto-sync to the cloud"""

        with self.jlock:
            j = self._get_journal(stream)
            j.add_rows_from_df(data)

    def flush_journals(self) -> None:
        """Called by the EdgeOrchestrator.upload_to_container function to flush all journals to disk and 
        onwards to archive"""

        logger.debug("Flushing all journals to disk")

        with self.jlock:
            for j in self._jpool.values():
                # Save the cached data to disk
                fname = j.save()

                # If there was no data, no file will have been created
                if not fname.exists():
                    logger.info(f"No data in {j.fname} when flushed")
                    continue

                # Move the file to the upload directory and append the timestamp
                ts = api.utc_to_fname_str()
                # We need a unique filename, so we append a number if the file already exists
                # We only hit this in testing, because there's usually half an hour between runs!
                i = 1
                target_fname = root_cfg.EDGE_UPLOAD_DIR.joinpath(f"{fname.stem}_{ts}{fname.suffix}")
                while target_fname.exists():
                    target_fname.replace(
                        target_fname.parent.joinpath(f"{target_fname.stem}{i!s}{target_fname.suffix}")
                    )
                    i += 1

                fname.rename(target_fname)
                # Delete the cached data from the Journal object
                j.delete()

    def stop(self) -> None:
        """Stop the LocalJournalPool, flush all data and exit any threads"""
        self.flush_journals()
        # No threads to stop

    def _get_journal(self, stream: Stream) -> Journal:
        """Generate the Journal filename for a DPtreeNodeCfg."""

        fname = file_naming.get_journal_filename(stream.type_id)
        if fname.name not in self._jpool:
            reqd_cols: list[str] = api.ALL_RECORD_ID_FIELDS 
            if stream.fields:
                reqd_cols.extend(stream.fields)
            j = Journal(fname, cached=True, reqd_columns=reqd_cols)
            self._jpool[fname.name] = j
        else:
            j = self._jpool[fname.name]

        return j
