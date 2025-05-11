########################################################################################
# Journal class utility for easily managing CSV type data
#
# Used for local file storage
########################################################################################
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from sensor_core import configuration as root_cfg


class Journal:
    _temp_fname = "_temp_fname"

    def __init__(self, 
                 fname: Optional[Path | str] = None, 
                 cached: bool=True, 
                 reqd_columns: Optional[list[str]]=None) -> None:
        """Constructor for the Journal class.

        Args:
            fname (Path | str, optional): The file name of the CSV file i
                if the file exists, it will be read-in;
                if it doesn't it will be created;
                if it's just a file name, it will be save to the cfg.DB_DIR directory. Defaults to None.
            cached (bool, optional):
                If True, the file is written to disk only when the save() method is called. Defaults to True.
                If False, the file is written to disk after each add_row(s) call.
            reqd_columns (list, optional): 
                A list of column names to save to the CSV file in the order specified.
                If None, the columns will be ordered randomly in the csv. Defaults to None.
        """
        self.reqd_columns = reqd_columns
        self._cached = cached
        self._data = pd.DataFrame()

        if fname is None:
            self.fname = Path(Journal._temp_fname)
        else:
            if isinstance(fname, str):
                fname = Path(fname)
            self.fname = fname
            if not fname.is_absolute():
                self.fname = root_cfg.EDGE_STAGING_DIR.joinpath(fname)
            if fname.exists():
                self._data = self.load()

    # Function to read a CSV file (to a list of dictionaries)
    def load(self) -> pd.DataFrame:
        try:
            self._data = pd.read_csv(self.fname)
        except pd.errors.EmptyDataError:
            self._data = pd.DataFrame()
        return self._data

    # Function to read data from a differnt CSV file and combine it with the existing Journal data
    def load_from_additional_file(self, fname: str) -> None:
        # Check the file exists and has data before loading it
        if Path(fname).exists() and Path(fname).stat().st_size > 0:
            try:
                df = pd.read_csv(fname)
                if not df.empty:
                    self._data = pd.concat([self._data, df])
                    if not self._cached:
                        self.save()
            except pd.errors.EmptyDataError:
                pass

    # Function to read data from a list of CSV files and combine them with the existing Journal data
    def load_from_additional_files(self, fnames: list[str]) -> None:
        dfs = []
        for fname in fnames:
            # Check the file exists and has data before loading it
            if Path(fname).exists() and Path(fname).stat().st_size > 0:
                try:
                    df = pd.read_csv(fname)
                    if not df.empty:
                        dfs.append(df)
                except pd.errors.EmptyDataError:
                    pass
        if dfs:
            self._data = pd.concat([self._data, *dfs])

    # Save the journal to a CSV file
    def save(self, fname: Optional[Path] = None) -> Path:
        if self._data.empty:
            return self.fname

        if fname is not None:
            self.fname = fname

        if self.fname == Path(Journal._temp_fname):
            raise ValueError("Cannot save Journal without a valid file name being provided.")

        if not self.fname.parent.exists():
            self.fname.parent.mkdir(parents=True, exist_ok=True)

        if self.reqd_columns is not None:
            # If some reqd_columns are not present in the data, add them with NaN values
            missing_columns = [col for col in self.reqd_columns if col not in self._data.columns]
            if missing_columns:
                for col in missing_columns:
                    self._data[col] = None
            self._data.to_csv(self.fname, index=False, columns=self.reqd_columns)
        else:
            self._data.to_csv(self.fname, index=False)

        return self.fname

    # Delete the journal file on disk and discard the data
    def delete(self) -> None:
        self._data = pd.DataFrame()
        if (self.fname != Journal._temp_fname) and self.fname.exists():
            os.remove(self.fname)

    # Add a row to the data list
    def add_row(self, row: dict) -> None:
        # Add a new row to the dataframe
        self._data = pd.concat([self._data, pd.DataFrame([row])], ignore_index=True)

        if not self._cached:
            self.save()

    # Add multiple rows to the data list
    def add_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        self._data = pd.concat([self._data, pd.DataFrame(rows)], ignore_index=True)
        if not self._cached:
            self.save()

    # Add multiple rows from a pandas dataframe
    def add_rows_from_df(self, df: pd.DataFrame) -> "Journal":
        self._data = pd.concat([self._data, df], ignore_index=True)
        if not self._cached:
            self.save()
        return self

    # Access the data list
    #
    # Normally this is returned as a copy, but for performance on read-only operations,
    # the copy can be disabled
    def get_data(self, copy: bool=True) -> list[dict]:
        if copy:
            return self._data.to_dict(orient="records")
        else:
            return self._data.to_dict(orient="records")

    # Access the data list as a dataframe
    #
    # Order the columns by providing a list of column names.
    # Doesn't need to include all columns names; any columns not in the list will be appended
    def as_df(self, column_order: Optional[list[str]]=None) -> pd.DataFrame:
        if column_order is None:
            return self._data
        else:
            return self._data[column_order]

    def cap_journal_size(self, size: int) -> None:
        # Cap the journal size to the specified size
        # Any rows beyond the size are removed
        self._data = self._data[:size]
