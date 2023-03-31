import pandas as pd
import pytest
from di_tella_2004_replication.analysis.weekly_panel_regression import (
    abs_regression_models_av_weekly,
    abs_regression_models_weekly,
)
from di_tella_2004_replication.config import BLD
from linearmodels.iv import absorbing


@pytest.fixture()
def weekly_data():
    return pd.read_pickle(BLD / "python" / "data" / "WeeklyPanel.pkl")


def test_abs_regression_models_weekly(weekly_data):
    abs_results = abs_regression_models_weekly(weekly_data, "robust")
    assert isinstance(abs_results, absorbing.AbsorbingLSResults)


def test_abs_regression_models_av_weekly(weekly_data):
    abs_results = abs_regression_models_av_weekly(weekly_data)
    assert isinstance(abs_results, absorbing.AbsorbingLSResults)
