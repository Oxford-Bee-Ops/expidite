"""Journal class utility for easily managing CSV type data.

Used for local file storage.
"""

import os
from pathlib import Path

import pandas as pd

from expidite_rpi.core import configuration as root_cfg


class Journal:
    def __init__(
        self,
        fname: Path,
        cached: bool = True,
        reqd_columns: list[str] | None = None,
    ) -> None:
        """Constructor for the Journal class.

        Args:
            fname (Path): The file name of the CSV file
                if the file exists, it will be read-in;
                if it doesn't it will be created;
                if it's just a file name, it will be saved to the staging directory.
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

        self.fname = fname
        if not fname.is_absolute():
            self.fname = root_cfg.EDGE_STAGING_DIR / fname
        if fname.exists():
            self._data = self._load()

    def _load(self) -> pd.DataFrame:
        """Function to read a CSV file (to a list of dictionaries)."""
        try:
            self._data = pd.read_csv(self.fname)
        except pd.errors.EmptyDataError:
            self._data = pd.DataFrame()
        return self._data

    def save(self) -> Path:
        """Save the journal to a CSV file."""
        if self._data.empty:
            return self.fname

        if not self.fname.parent.exists():
            self.fname.parent.mkdir(parents=True, exist_ok=True)

        if self.reqd_columns is not None:
            # If some reqd_columns are not present in the data, add them with NaN values.
            missing_columns = [col for col in self.reqd_columns if col not in self._data.columns]
            if missing_columns:
                for col in missing_columns:
                    self._data[col] = None
            self._data.to_csv(self.fname, index=False, columns=self.reqd_columns)
        else:
            self._data.to_csv(self.fname, index=False)

        return self.fname

    def delete(self) -> None:
        """Delete the journal file on disk and discard the data."""
        self._data = pd.DataFrame()
        if self.fname.exists():
            os.remove(self.fname)

    def add_row(self, row: dict) -> None:
        """Add a row to the data list."""
        # Add a new row to the dataframe
        self._data = pd.concat([self._data, pd.DataFrame([row])], ignore_index=True)

        if not self._cached:
            self.save()

    def add_rows(self, rows: list[dict]) -> None:
        """Add multiple rows to the data list."""
        if not rows:
            return
        self._data = pd.concat([self._data, pd.DataFrame(rows)], ignore_index=True)
        if not self._cached:
            self.save()

    def add_rows_from_df(self, df: pd.DataFrame) -> "Journal":
        """Add multiple rows from a pandas dataframe."""
        self._data = pd.concat([self._data, df], ignore_index=True)
        if not self._cached:
            self.save()
        return self

    def get_data(self) -> list[dict]:
        """Access the data list. This is returned as a copy."""
        return self._data.to_dict(orient="records")

    def as_df(self, column_order: list[str] | None = None) -> pd.DataFrame:
        """Access the data list as a dataframe.

        Order the columns by providing a list of column names.
        Doesn't need to include all columns names; any columns not in the list will be appended.
        """
        if column_order is None:
            return self._data
        return self._data[column_order]
