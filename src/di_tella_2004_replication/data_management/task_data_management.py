"""Tasks for managing the data."""

import pandas as pd
import pyreadstat
import pytask

from di_tella_2004_replication.config import BLD, SRC
from di_tella_2004_replication.data_management.clean_data import process_crimebyblock


@pytask.mark.depends_on(
    {
        "scripts": ["clean_data.py"],
        "CrimeByBlock": SRC / "data" / "CrimebyBlock.dta",
    },
)
@pytask.mark.produces(BLD / "python" / "data" / "CrimeByBlock_panel.csv")
def task_clean_data_python(depends_on, produces):
    """Clean the data (Python version)."""
    data = pd.read_csv(depends_on["data"])
    data, meta = pyreadstat.read_dta(depends_on["CrimeByBlock"])
    data = process_crimebyblock(data)
    data.to_csv(produces, index=False)
