from pathlib import Path
from queue import Queue
from threading import Event, Lock, Timer
from typing import Optional

import pandas as pd

from sensor_core import api
from sensor_core import configuration as root_cfg
from sensor_core.cloud_connector import CloudConnector
from sensor_core.utils.journal import Journal

logger = root_cfg.setup_logger(name="sensor_core")


class _CloudJournalManager:
    _instance = None
    """CloudJournalManager is a worker thread that manages synchronisation of CloudJournal 
    objects to the cloud.
    
    It should only be instantiated or called by a CloudJournal object.
    We use a single worker thread and queues to ensure thread-safety of local processing."""

    def __init__(self, cloud_container: str) -> None:
        super().__init__()
        self._journals: dict[CloudJournal, Queue] = {}
        self._journals_dict_lock = Lock() # grab this lock when modifying the _journals dict
        self.cloud_container = cloud_container
        self._stop_requested = Event()
        self._sync_timer = Timer(root_cfg.JOURNAL_SYNC_FREQUENCY, self.sync_run)
        self._sync_timer.start()

    @staticmethod
    def get(cloud_container: str)-> "_CloudJournalManager":
        """Get the singleton worker thread"""

        if _CloudJournalManager._instance is None:
            _CloudJournalManager._instance = _CloudJournalManager(cloud_container)
        return _CloudJournalManager._instance

    def sync_run(self) -> None:
        """Persistently manage synchronisation of the CloudJournal objects to the cloud

        Alternatively, the user can use flush() to actively manage synchronization."""

        try:
            self.flush_all()
        except Exception as e:
            logger.error(f"Error in CloudJournalManager sync_run: {e}", exc_info=True)
        finally:
            logger.debug("Schedule next CloudJournalManager sync timer")
            if not self._stop_requested.is_set():
                self._sync_timer = Timer(root_cfg.JOURNAL_SYNC_FREQUENCY, self.sync_run)
                self._sync_timer.name = "cj_sync_timer"
                self._sync_timer.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self._sync_timer.cancel()

    def add(self, journal: "CloudJournal", data: list[dict]) -> None:
        """Add data to the local data queue

        Parameters
        ----------
        - journal: CloudJournal to which data should be added
        - data: list[dict] of data to add
        """
        jqueue: Queue
        if journal not in self._journals:
            jqueue = Queue()
            with self._journals_dict_lock:
                self._journals[journal] = jqueue
        else:
            jqueue = self._journals[journal]
        jqueue.put(data)

    def flush_all(self) -> None:
        """Attempt to sync all the queued data to the remote journals.

        Blocks until uploads are complete or fail."""

        # We get the instance locally rather than storing it in self because it avoids issues
        # when we change between cloud types during testing.
        cc = CloudConnector.get_instance(root_cfg.CLOUD_TYPE)

        # We use a lock to ensure that only one thread can flush at a time
        # Otherwise we end up with a RuntimeError: dictionary changed size during iteration
        with self._journals_dict_lock:
            start_time = api.utc_now()
            logger.debug(f"Starting flush at {start_time}")
            for journal, jqueue in self._journals.items():
                assert isinstance(journal, CloudJournal)
                assert isinstance(jqueue, Queue)
                # Add the items in the queue to a local file that we can then append to the cloud file
                lj = Journal(journal.local_fname,
                            reqd_columns=journal.reqd_columns)
                empty = True
                while not jqueue.empty():
                    data_list_dict: list[dict] = jqueue.get()
                    lj.add_rows(data_list_dict)
                    empty = False
    
                if not empty:
                    # The Journal.save() function drops any columns that are not in the reqd_columns list
                    lj.save()

                    # Append the contents of lj to the cloud blob
                    cc.append_to_cloud(journal.cloud_container, 
                                        journal.local_fname,
                                        delete_src=True)

            time_diff = (api.utc_now() - start_time).total_seconds()
            logger.debug(f"Completed flush_all started at {start_time} after {time_diff} seconds")


class CloudJournal:
    """CloudJournal provides thread-safe storage and retrieval of log / CSV-type data mastered in a 
    cloud datastore.

    Threads can safely append data using add() methods.
    Data is periodically synchronised with the cloud-mastered file, automatically or on flush().
    The CloudJournal is a thick API for the CloudJournalManager which does the work.
    """

    def __init__(self, local_fname: Path, cloud_container: str, reqd_columns: Optional[list[str]]) -> None:
        """Creates a CloudJournal instance uniquely identified by the local fname Path and the
        remote CloudContainer which contains the master data.

        Neither the local nor the cloud files need exist; they will be created as required,
        but the local and cloud directories must exist.

        Parameters
        ----------
        - local_fname: The local file name to use for the journal. Must be an absolute path.
        - cloud_container: The CloudContainer where the cloud file resides.
        - reqd_columns: A list of column names to save to the CSV file in the order specified.
            If None, the order of the columns in the csv is undefined.
        """
        if isinstance(local_fname, str):
            local_fname = Path(local_fname)
        assert local_fname.is_absolute()
        assert reqd_columns is not None
        assert len(reqd_columns) > 0

        self.manager = _CloudJournalManager.get(cloud_container)
        self.local_fname = local_fname
        self.cloud_filename = local_fname.name
        self.cloud_container = cloud_container
        self.reqd_columns = reqd_columns

    # Function to read a remote CSV file into a local journal
    def download(self) -> list[dict]:
        # Originally implemented with DictReader, but switched to Pandas to get auto-detect of numeric types
        try:
            # We get the instance locally rather than storing it in self because it avoids issues
            # when we change between cloud types during testing.
            cc = CloudConnector.get_instance(root_cfg.CLOUD_TYPE)
            cc.download_from_container(
                self.cloud_container, self.cloud_filename, self.local_fname
            )
            file_as_df = pd.read_csv(self.local_fname)
            self._data = file_as_df.to_dict(orient="records")
            return self._data
        except pd.errors.EmptyDataError:
            return []

    # Save the queued additions to the master journal
    def flush_all(self) -> None:
        self.manager.flush_all()

    # Stop the worker thread
    def stop(self) -> None:
        self.manager.stop()

    # Add a row to the data list
    def add_row(self, row: dict) -> None:
        self.manager.add(self, [row])

    # Add multiple rows to the data list
    def add_rows(self, rows: list[dict]) -> None:
        self.manager.add(self, rows)

    # Add multiple rows from a pandas dataframe
    def add_rows_from_df(self, df: pd.DataFrame) -> None:
        self.add_rows(df.to_dict(orient="records"))

    # Access the data list
    #
    # Normally this is returned as a copy, but for performance on read-only operations,
    # the copy can be disabled
    def get_data(self, copy:bool=True) -> list[dict]:
        if copy:
            return self._data.copy()
        else:
            return self._data

    # Access the data list as a dataframe
    #
    # Order the columns by providing a list of column names.
    # Doesn't need to include all columns names; any columns not in the list will be appended
    def as_df(self, column_order: Optional[list[str]]=None) -> pd.DataFrame:
        df = pd.DataFrame(self._data)
        if column_order is not None:
            df = df[column_order + [col for col in df.columns if col not in column_order]]
        return df
