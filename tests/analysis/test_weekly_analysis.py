import pandas as pd
import pytest
from di_tella_2004_replication.analysis.weekly_panel_regression import (
    abs_regression_models_weekly,
)
from di_tella_2004_replication.config import BLD


@pytest.fixture()
def weekly_data():
    return pd.read_pickle(BLD / "python" / "data" / "WeeklyPanel.pkl")


def test_abs_regression_models_weekly(weekly_data):
    abs_results = abs_regression_models_weekly(weekly_data, "robust")
    assert isinstance(abs_results, sm_panel.PanelResults)
