import pandas as pd
import pyreadstat
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_crime_by_block import (
    _calculate_theft_differences,
    _calculate_total_theft_by_suffix,
    _clean_column_names_block,
    _convert_dtypes,
    _create_new_variables_ind,
    _drop_repeated_obs,
    _split_theft_data,
)


@pytest.fixture()
def original_data():
    data, meta = pyreadstat.read_dta(SRC / "data" / "CrimeByBlock.dta")
    return data


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
            "distrito": ["A", "B", "C"],
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


@pytest.fixture()
def expected_total_theft_by_suffix():
    return {
        "theft_hv1": 2,
        "theft_lv1": 1,
        "theft_night1": 2,
        "theft_day1": 1,
        "theft_weekend1": 1,
        "theft_weekday1": 2,
    }


@pytest.fixture()
def ind_char_new_variables():
    return pd.DataFrame(
        {
            "jewish_inst": [0, 1, 0],
            "non_unmet_basic_needs_rate": [0.2, 0.1, 0.3],
            "non_overcrowd_rate": [0.4, 0.5, 0.3],
            "employment_rate": [0.8, 0.9, 0.7],
            "census_district": [1, 1, 2],
            "census_tract": [1, 2, 1],
        },
    )


def test_clean_column_names(test_df, expected_df):
    df_cleaned = _clean_column_names_block(test_df)
    assert list(df_cleaned.columns) == list(expected_df.columns)


def test_clean_column_names_data(test_df, expected_df):
    pd.testing.assert_frame_equal(_clean_column_names_block(test_df), expected_df)


def test_clean_colum_names_rob_case(test_df):
    df_cleaned = _clean_column_names_block(test_df)
    assert all("rob" not in col_name for col_name in df_cleaned.columns)


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


def test_split_and_calculate_total_theft_by_suffix(
    theft_df,
    expected_total_theft_by_suffix,
):
    theft_data = _split_theft_data(theft_df, month=1, maxrange=3)
    theft_data = _calculate_total_theft_by_suffix(theft_data, month=1)


def test_create_new_variables_ind(ind_char_new_variables):
    df_new = _create_new_variables_ind(ind_char_new_variables)
    tol = 1e-8
    assert all(abs(df_new["unmet_basic_needs_rate"] - [0.8, 0.9, 0.7]) < tol)
    assert all(abs(df_new["overcrowd_rate"] - [0.6, 0.5, 0.7]) < tol)
    assert all(abs(df_new["unemployment_rate"] - [0.2, 0.1, 0.3]) < tol)
    assert df_new[["census_district", "census_tract"]].values.tolist() == [
        [1, 1],
        [1, 2],
        [2, 1],
    ]


def test__drop_repated_obs_unique_obs(ind_char_new_variables):
    df_new = _create_new_variables_ind(ind_char_new_variables)
    df_unique = _drop_repeated_obs(df_new)
    assert not df_unique.duplicated(subset=["census_district", "census_tract"]).any()


import pandas as pd


def test_calculate_theft_differences():
    # Create a sample DataFrame for testing
    data = {
        "tot_theft_hv3": [10, 20, 30],
        "tot_theft_lv3": [2, 4, 6],
        "tot_theft_night3": [6, 12, 18],
        "tot_theft_day3": [1, 2, 3],
        "tot_theft_weekday3": [9, 18, 27],
        "tot_theft_weekend3": [1, 2, 3],
    }
    df = pd.DataFrame(data)

    # Call the function being tested
    result = _calculate_theft_differences(df, 3)

    # Define the expected output
    expected_output = {
        "tot_theft_hv3": [10, 20, 30],
        "tot_theft_lv3": [2, 4, 6],
        "tot_theft_night3": [6, 12, 18],
        "tot_theft_day3": [1, 2, 3],
        "tot_theft_weekday3": [9, 18, 27],
        "tot_theft_weekend3": [1, 2, 3],
        "dif_hv_lv3": [8, 16, 24],
        "dif_night_day3": [5, 10, 15],
        "dif_weekday_weekend3": [8, 16, 24],
    }
    expected_output_df = pd.DataFrame(expected_output)

    # Compare the actual and expected outputs
    pd.testing.assert_frame_equal(result, expected_output_df)
