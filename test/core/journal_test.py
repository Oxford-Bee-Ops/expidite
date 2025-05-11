import os

import pandas as pd
import pytest
from sensor_core import configuration as root_cfg
from sensor_core.utils.journal import Journal

logger = root_cfg.setup_logger("sensor_core")
root_cfg.TEST_MODE = root_cfg.MODE.TEST


class Test_journal:
    @pytest.mark.quick
    def test_journal_basics(self) -> None:
        tmp_dir = root_cfg.TMP_DIR
        test_file = tmp_dir.joinpath("test.csv")
        if test_file.exists():
            os.remove(test_file)
        test_input = {"key1": "value1", "key2": "value2"}

        # Create the journal object
        j = Journal(test_file, cached=False)

        # Add_row; because it's not cached, the file should be written to disk
        j.add_row(test_input)
        assert j.get_data() == [test_input]
        assert test_file.exists()

        # Save it explicitly to disk
        j.save()

        # Add a second identical row
        j.add_row(test_input)
        assert len(j.get_data()) == 2

        # Add a row with different keys
        test_input2 = {"key2": "value3", "key3": "value3"}
        j.add_row(test_input2)
        assert len(j.get_data()) == 3

        # Add multiple rows
        test_input3 = [
            {"key1": "value1", "key2": "value2"},
            {"key2": "value4", "key4": "value4"},
        ]
        j.add_rows(test_input3)
        assert len(j.get_data()) == 5
        # Get the list of dictionaries and calculate the number of unique keys in all dictionaries
        keys = set().union(*(d.keys() for d in j.get_data()))
        assert len(keys) == 4

        # Add multiple rows from a pandas dataframe
        df = pd.DataFrame(test_input3)
        j.add_rows_from_df(df)
        assert len(j.get_data()) == 7

        # Re-load the data from disk
        j2 = Journal(test_file, cached=False)
        assert len(j2.get_data()) == 7
        keys = set().union(*(d.keys() for d in j.get_data()))
        assert len(keys) == 4
        df = j2.as_df(["key1", "key2", "key3"])
        print(df)

        # Delete the journal file
        j.delete()
        assert not test_file.exists()

    @pytest.mark.quick
    def test_journal_existing(self) -> None:
        tmp_dir = root_cfg.TMP_DIR
        test_file = tmp_dir.joinpath("test.csv")
        if test_file.exists():
            os.remove(test_file)
        test_input = {"key1": "value1", "key2": "value2"}

        # Create journal object
        j = Journal(test_file, cached=False)
        j.add_row(test_input)
        assert len(j.get_data()) == 1

        j2 = Journal(test_file, cached=False)
        j2.add_row(test_input)
        assert len(j2.get_data()) == 2

        # We don't support having multiple instances of the same journal file
        # open at the same time.  We could, but we'd need to reload on every add_ operation
        # So this test should return 2 (ie the original 1 row, plus the new row) not 3.
        j.add_row(test_input)
        assert len(j.get_data()) == 2
