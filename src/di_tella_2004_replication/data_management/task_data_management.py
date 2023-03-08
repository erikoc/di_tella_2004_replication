"""Tasks for managing the data."""

import pandas as pd
import pytask

from di_tella_2004_replication.config import BLD, SRC
from di_tella_2004_replication.data_management import clean_data
from di_tella_2004_replication.utilities import read_yaml


@pytask.mark.depends_on(
    {
        "scripts": ["clean_data.py"],
        "CrimeByBlock": SRC / "data" / "CrimebyBlock.dta",
    },
)
@pytask.mark.produces(BLD / "python" / "data" / "data_clean.csv")
def task_clean_data_python(depends_on, produces):
    """Clean the data (Python version)."""
    data_info = read_yaml(depends_on["data_info"])
    data = pd.read_csv(depends_on["data"])
    data = clean_data(data, data_info)
    data.to_csv(produces, index=False)
