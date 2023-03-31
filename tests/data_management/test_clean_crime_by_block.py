import pandas as pd
import pyreadstat
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_crime_by_block import (
    _calculate_theft_differences,
    _calculate_total_theft_by_suffix,
    _clean_column_names_block,
    _convert_dtypes,
    _create_new_variables,
    _create_new_variables_ind,
    _create_panel_data,
    _split_theft_data,
    process_crime_by_block,
    process_ind_char_data,
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


def test_calculate_total_thefts_by_suffix(original_data):
    df = _clean_column_names_block(original_data)
    df = _convert_dtypes(df)

    theft_data = df.loc[:, df.columns.str.startswith("theft")]

    for i in range(1, 24):
        theft_data.loc[theft_data[f"theft{i}corner"] == 1, f"theft{i}"] = 0.25

    for month in range(4, 13):
        theft_data = _split_theft_data(theft_data, month)
        theft_data = _calculate_total_theft_by_suffix(theft_data, month)
        cols_list = [
            f"tot_theft_{suffix}{month}"
            for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]
        ]

    assert set(cols_list).issubset(set(theft_data.columns))


def test_create_panel_data_and_new_variables(original_data):
    df = _clean_column_names_block(original_data)
    df = _convert_dtypes(df)

    theft_data = df.loc[:, df.columns.str.startswith("theft")]
    ind_char_data = df[[col for col in df.columns if not col.startswith("theft")]]

    for i in range(1, 24):
        theft_data.loc[theft_data[f"theft{i}corner"] == 1, f"theft{i}"] = 0.25

    for month in range(4, 13):
        theft_data = _split_theft_data(theft_data, month)
        theft_data = _calculate_total_theft_by_suffix(theft_data, month)
        theft_data = _calculate_theft_differences(theft_data, month)

    theft_cols = [col for col in theft_data.columns if col.startswith("theft")]
    theft_data = theft_data.drop(columns=theft_cols)
    theft_data = theft_data.reset_index(names=["block"])

    crime_by_block_panel = _create_panel_data(ind_char_data, theft_data)
    crime_by_block_panel = _create_new_variables(
        crime_by_block_panel,
        time_variable="month",
        event_time=7,
    )

    assert not (
        (crime_by_block_panel["treatment"] == 0).all()
        and (crime_by_block_panel["treatment_1d"] == 0).all()
        and (crime_by_block_panel["treatment_2d"] == 0).all()
    )

    prefixes = [
        "tot_theft_hv",
        "tot_theft_lv",
        "dif_hv_lv",
        "tot_theft_night",
        "tot_theft_day",
        "dif_night_day",
        "tot_theft_weekday",
        "tot_theft_weekend",
        "dif_weekday_weekend",
    ]
    for prefix in prefixes:
        matching_columns = [
            col for col in crime_by_block_panel.columns if col.startswith(prefix)
        ]
        assert (
            len(matching_columns) == 1
        ), f"Found {len(matching_columns)} columns that start with {prefix}"


def test_process_crime_by_block(original_data):
    df = process_crime_by_block(original_data)
    assert not any(col.startswith("theft") for col in df.columns)
    assert len(df) == 876 * 9


def test_process_ind_char_data(original_data):
    df = process_ind_char_data(original_data)
    assert not any(col.startswith("theft") for col in df.columns)
    assert not df.duplicated(subset=["census_district", "census_tract"]).any()
