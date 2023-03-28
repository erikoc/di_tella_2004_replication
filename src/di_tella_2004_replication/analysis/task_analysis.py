"""Tasks running the core analyses."""
import pickle

import pandas as pd
import pytask

from di_tella_2004_replication.analysis.crime_by_block_regression import (
    abs_regression_models_dif,
    abs_regression_models_totals,
    fe_regression_models_dif,
    fe_regression_models_totals,
)
from di_tella_2004_replication.config import BLD


@pytask.mark.depends_on(BLD / "python" / "data" / "CrimeByBlockPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "fe_tot_models.pickle")
def task_fit_fe_totals_python(depends_on, produces):
    data = pd.read_pickle(depends_on)
    model = fe_regression_models_totals(data)
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "CrimeByBlockPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "fe_dif_models.pickle")
def task_fit_fe_dif_python(depends_on, produces):
    data = pd.read_pickle(depends_on)
    model = fe_regression_models_dif(data)
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "CrimeByBlockPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "abs_tot_models.pickle")
def task_fit_abs_tot_python(depends_on, produces):
    data = pd.read_pickle(depends_on)
    model = abs_regression_models_totals(data)
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "CrimeByBlockPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "abs_dif_models.pickle")
def task_fit_abs_dif_python(depends_on, produces):
    data = pd.read_pickle(depends_on)
    model = abs_regression_models_dif(data)
    with open(produces, "wb") as f:
        pickle.dump(model, f)
