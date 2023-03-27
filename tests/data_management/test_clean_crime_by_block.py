import pandas as pd
import pyreadstat
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_crime_by_block import (
    _clean_column_names,
    _convert_dtypes,
    _split_theft_data,
)


@pytest.fixture()
def original_data():
    return {
        "crime_by_block": pyreadstat.read_dta(SRC / "data" / "CrimeByBLock.dta"),
    }


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
def theft_df():
    return pd.DataFrame(
        {
            "block": [1, 2],
            "theft1": [1, 1],
            "theft1val": [5000, 10000],
            "theft2": [0, 0.25],
            "theft2val": [None, 50000],
            "theft1hour": [5, 12],
            "theft2hour": [None, 6],
            "theft1day": [1, 7],
            "theft2day": [None, 3],
            "theft1month": [1, 1],
            "theft2month": [None, 2],
        },
    )


@pytest.fixture()
def theft_df_expected_cols():
    return [
        "block",
        "theft1",
        "theft1val",
        "theft2",
        "theft2val",
        "theft1hour",
        "theft2hour",
        "theft1day",
        "theft2day",
        "theft1month",
        "theft2month",
        "theft_hv11",
        "theft_lv11",
        "theft_night11",
        "theft_day11",
        "theft_weekday11",
        "theft_weekend11",
        "theft_hv21",
        "theft_lv21",
        "theft_night21",
        "theft_day21",
        "theft_weekday21",
        "theft_weekend21",
    ]


def test_clean_column_names(test_df, expected_df):
    df_cleaned = _clean_column_names(test_df)
    assert list(df_cleaned.columns) == list(expected_df.columns)


def test_clean_column_names_data(test_df, expected_df):
    pd.testing.assert_frame_equal(_clean_column_names(test_df), expected_df)


def test_convert_dtypes(expected_df):
    converted_df = _convert_dtypes(expected_df, maxrange=3)
    assert all(
        converted_df[col].dtype == float
        for col in ["theft1", "theft2", "theft1val", "theft2val"]
    )


def test_split_theft_data_shape(theft_df):
    theft_data = _split_theft_data(theft_df, month=1, maxrange=3)
    assert theft_data.shape == (2, 23)


def test_split_theft_data_cols(theft_df, theft_df_expected_cols):
    theft_data = _split_theft_data(theft_df, month=1, maxrange=3)
    assert list(theft_data.columns) == theft_df_expected_cols
