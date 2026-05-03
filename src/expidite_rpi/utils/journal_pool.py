from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from threading import RLock

import pandas as pd

from expidite_rpi.core import api, file_naming
from expidite_rpi.core import configuration as root_cfg
from expidite_rpi.core.dp_config_objects import Stream
from expidite_rpi.utils.cloud_journal import CloudJournal

logger = root_cfg.setup_logger("expidite")


class JournalPool(ABC):
    """The JournalPool is responsible for thread-safe saving of data to journal files and onward to archive.

    The JournalPool is a singleton shared by all DPtreeNode instances and should be retrieved using
    JournalPool.get().

    There is a "Journal" in the JournalPool per DPtreeNode type_id. Data is stored based on its
    bapi.RECORD_ID.DS_TYPE_ID and bapi.RECORD_ID.TIMESTAMP in the CJ.
    """

    _instance: JournalPool | None = None

    @staticmethod
    def get() -> JournalPool:
        """Get the singleton instance of the JournalPool."""
        if JournalPool._instance is None:
            logger.debug("Creating JournalPool instance")
            JournalPool._instance = CloudJournalPool()
        return JournalPool._instance

    @abstractmethod
    def add_rows(self, stream: Stream, data: list[dict], timestamp: datetime | None = None) -> None:
        """Add data rows as a list of dictionaries.

        The fields in each dictionary must match the DPtreeNodeCfg reqd_fields.
        """
        msg = "Abstract method needs to be implemented"
        raise AssertionError(msg)

    @abstractmethod
    def add_rows_from_df(self, stream: Stream, data: pd.DataFrame, timestamp: datetime | None = None) -> None:
        """Add data in the form of a Pandas DataFrame to the Journal, which will auto-sync to the cloud.

        All data MUST relate to the same DAY as timestamp.
        """
        msg = "Abstract method needs to be implemented"
        raise AssertionError(msg)

    @abstractmethod
    def flush_journals(self) -> None:
        """Flush all journals to disk and onwards to archive."""
        msg = "Abstract method needs to be implemented"
        raise AssertionError(msg)

    @abstractmethod
    def stop(self) -> None:
        """Stop the JournalPool, flush all data and exit any threads."""
        msg = "Abstract method needs to be implemented"
        raise AssertionError(msg)


class CloudJournalPool(JournalPool):
    """The CloudJournalPool is a concrete implementation of a JournalPool for running in EDGE mode.

    It is based on a pool of CloudJournal instances.
    """

    def __init__(self) -> None:
        self._cj_pool: dict[str, CloudJournal] = {}
        self.jlock = RLock()

    def add_rows(self, stream: Stream, data: list[dict], timestamp: datetime | None = None) -> None:
        """Add data to the appropriate CloudJournal, which will auto-sync to the cloud.

        All data MUST relate to the same DAY as timestamp.
        """
        assert timestamp is not None, "Timestamp must be provided for add_rows_from_df with CloudJournalPool"
        logger.debug(f"Lock: adding rows for stream {stream.type_id}")
        with self.jlock:
            cj = self._get_journal(stream, timestamp)
            cj.add_rows(data)
        logger.debug(f"Unlock: added rows for stream {stream.type_id}")

    def add_rows_from_df(self, stream: Stream, data: pd.DataFrame, timestamp: datetime | None = None) -> None:
        """Add data to the appropriate CloudJournal, which will auto-sync to the cloud.

        All data MUST relate to the same DAY as timestamp.
        """
        if timestamp is None:
            timestamp = api.utc_now()

        logger.debug(f"Lock: adding rows for stream {stream.type_id}")
        with self.jlock:
            cj = self._get_journal(stream, timestamp)
            cj.add_rows_from_df(data)
        logger.debug(f"Unlock: added rows for stream {stream.type_id}")

    def flush_journals(self) -> None:
        """Flush all journals to disk and onwards to archive."""
        logger.debug("Lock: flush_journals")
        with self.jlock:
            # We can call flush_all on any CloudJournal in the pool and all will get flushed
            for cj in self._cj_pool.values():
                cj.flush_all()
                break
        logger.debug("Unlock: flush_journals")

    def stop(self) -> None:
        """Stop the CloudJournalPool, flush all data and exit any threads."""
        logger.debug("Lock: stopping CloudJournalPool")
        with self.jlock:
            # We can call stop on any CloudJournal in the pool and all will get stopped
            for cj in self._cj_pool.values():
                cj.stop()
                break
        logger.debug("Unlock: stopped CloudJournalPool")

    def _get_journal(self, stream: Stream, day: datetime) -> CloudJournal:
        """Generate the CloudJournal filename for a DPtreeNodeCfg.

        The V3 filename format is:
            V3_{DPtreeNodeCfg_type_id}_{day}.csv
        """
        # Check that the output_fields contain at least all the bapi.REQD_RECORD_ID_FIELDS
        assert stream.fields is not None, f"output_fields must be set in {stream}"

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
