import pandas as pd
import pytest
from di_tella_2004_replication.analysis.crime_by_block_regression import (
    abs_regression_models_dif,
    abs_regression_models_totals,
    fe_regression_models_dif,
    fe_regression_models_totals,
)
from di_tella_2004_replication.analysis.crime_by_block_stats import (
    neighborhood_comparison_tables,
    t_tests_crime_by_block,
)
from di_tella_2004_replication.config import BLD


@pytest.fixture()
def crime_data():
    return pd.read_pickle(BLD / "python" / "data" / "CrimeByBlockPanel.pkl")


@pytest.fixture()
def crime_ind_char_data():
    return pd.read_pickle(BLD / "python" / "data" / "CrimeByBlockIndChar.pkl")


def test_fe_tot_reg(crime_data):
    fe_models = fe_regression_models_totals(crime_data)
    assert len(fe_models) == 6


def test_abs_tot_reg(crime_data):
    abs_models = abs_regression_models_totals(crime_data)
    assert len(abs_models) == 6


def test_fe_dif_reg(crime_data):
    fe_models_dif = abs_regression_models_dif(crime_data)
    assert len(fe_models_dif) == 3


def test_abs_dif_reg(crime_data):
    abs_models_dif = fe_regression_models_dif(crime_data)
    assert len(abs_models_dif) == 3


def test_neighborhood_comparison_tables(crime_ind_char_data):
    test_tables = neighborhood_comparison_tables(crime_ind_char_data)
    assert test_tables.shape == (3, 24)


def test_t_test_ind_char_data(crime_ind_char_data):
    test_results = t_tests_crime_by_block(crime_ind_char_data)
    assert test_results.shape == (2, 8)
