import pandas as pd
import pytest
from di_tella_2004_replication.data_management.clean_data import (
    _clean_column_names,
    _convert_dtypes,
)


@pytest.fixture()
def test_df():
    return pd.DataFrame(
        {
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
            "theft1": [1, 1, 0],
            "theft1val": [300, 250, 200],
            "week_day": ["Monday", "Tuesday", "Wednesday"],
            "census_district": ["A", "B", "C"],
        },
    )


def test_clean_column_names(test_df, expected_df):
    df_cleaned = _clean_column_names(test_df)
    assert list(df_cleaned.columns) == list(expected_df.columns)


def test_clean_column_names_data(test_df, expected_df):
    pd.testing.assert_frame_equal(_clean_column_names(test_df), expected_df)
    assert isinstance(_clean_column_names(test_df), pd.DataFrame)


def test_convert_dtypes(expected_df):
    converted_df = _convert_dtypes(expected_df)
    for i in range(1, 23):
        assert isinstance(converted_df[f"theft{i}"], float)
        assert isinstance(converted_df[f"theft{i}val"], float)
