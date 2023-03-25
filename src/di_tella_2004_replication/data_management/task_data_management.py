"""Tasks for managing the data."""

import pyreadstat
import pytask

from di_tella_2004_replication.config import BLD, SRC
from di_tella_2004_replication.data_management.clean_crime_by_block import (
    process_crime_by_block,
)


@pytask.mark.depends_on(
    {
        "scripts": ["clean_data.py"],
        "CrimeByBlock": SRC / "data" / "CrimebyBlock.dta",
    },
)
@pytask.mark.produces(BLD / "python" / "data" / "CrimeByBlock_Panel.pkl")
def task_clean_data_python(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on["CrimeByBlock"])
    data = process_crime_by_block(data)
    data.to_pickle(produces)
