"""Tasks for managing the data."""
import pyreadstat
import pytask

from di_tella_2004_replication.config import BLD, SRC
from di_tella_2004_replication.data_management.clean_crime_by_block import (
    process_crime_by_block,
    process_weekly_panel,
)


@pytask.mark.produces(
    BLD / "python" / "data" / "CrimeByBlockPanel.pkl",
)
@pytask.mark.depends_on(SRC / "data" / "CrimebyBlock.dta")
def task_process_crime_by_bloc_python(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on)
    crime_data = process_crime_by_block(data)
    crime_data.to_pickle(produces)


@pytask.mark.produces(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.depends_on(SRC / "data" / "WeeklyPanel.dta")
def task_process_weekly_panel_python(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on)
    weekly_panel = process_weekly_panel(data)
    weekly_panel.to_pickle(produces)
