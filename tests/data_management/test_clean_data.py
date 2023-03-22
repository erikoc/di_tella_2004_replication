import pandas as pd
import pyreadstat
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_data import (
    _clean_column_names,
    _convert_dtypes,
)


@pytest.fixture()
def test_df():
    return pd.DataFrame(
        {
            "observ": [1, 2, 3],
            "rob1": [1, 0, 1],
            "rob1val": [100, 150, 200],
            "rob2": [1, 1, 0],
            "rob2val": [300, 250, 200],
            "day": ["Monday", "Tuesday", "Wednesday"],
            "district": ["A", "B", "C"],
        },
    )


@pytest.fixture()
def expected_df():
    return pd.DataFrame(
        {
            "block": [1, 2, 3],
            "theft1": [1, 0, 1],
            "theft1val": [100, 150, 200],
            "theft2": [1, 1, 0],
            "theft2val": [300, 250, 200],
            "week_day": ["Monday", "Tuesday", "Wednesday"],
            "census_district": ["A", "B", "C"],
        },
    )


@pytest.fixture()
def original_data_input():
    data, meta = pyreadstat.read_dta(SRC / "data" / "CrimebyBlock.dta")
    return data


def test_clean_column_names(test_df, expected_df):
    df_cleaned = _clean_column_names(test_df)
    assert list(df_cleaned.columns) == list(expected_df.columns)


def test_clean_column_names_data(test_df, expected_df):
    pd.testing.assert_frame_equal(_clean_column_names(test_df), expected_df)


def test_convert_dtypes(expected_df):
    converted_df = _convert_dtypes(expected_df)
    theft_cols = [
        col
        for col in converted_df.columns
        if col.startswith("theft") and ("val" in col or col[-1].isdigit())
    ]
    for col in theft_cols:
        assert converted_df[col].dtypes == float
