"""Tasks for managing the data."""
import pyreadstat
import pytask
import pandas as pd


"""Crime by Block and WeeklyPanel"""

from di_tella_2004_replication.config import BLD, SRC
from di_tella_2004_replication.data_management.clean_crime_by_block import (
    process_crime_by_block,
    process_ind_char_data,
    process_weekly_panel,
)


@pytask.mark.produces(BLD / "python" / "data" / "CrimeByBlockPanel.pkl")
@pytask.mark.depends_on(SRC / "data" / "CrimebyBlock.dta")
def task_process_crime_by_block_python(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on)
    crime_data = process_crime_by_block(data)
    crime_data.to_pickle(produces)


@pytask.mark.produces(BLD / "python" / "data" / "CrimeByBlockIndChar.pkl")
@pytask.mark.depends_on(SRC / "data" / "CrimebyBlock.dta")
def task_process_ind_char_python(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on)
    ind_char_data = process_ind_char_data(data)
    ind_char_data.to_pickle(produces)


@pytask.mark.produces(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.depends_on(SRC / "data" / "WeeklyPanel.dta")
def task_process_weekly_panel_python(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on)
    weekly_panel = process_weekly_panel(data)
    weekly_panel.to_pickle(produces)


"""Monthly Panel"""

from di_tella_2004_replication.config import BLD, SRC
from di_tella_2004_replication.data_management.clean_MonthlyPanel import (
    monthlypanel_1,
    monthlypanel_2,
    monthlypanel_3,
    monthlypanel_new,
)


@pytask.mark.produces(BLD / "python" / "data" / "MonthlyPanel.pkl")
@pytask.mark.depends_on(SRC / "data" / "MonthlyPanel.dta")
def task_monthlypanel_1(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on)
    MonthlyPanel = monthlypanel_1(data)
    MonthlyPanel.to_pickle(produces)
 
    
@pytask.mark.produces(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel.pkl")
def task_monthlypanel_2(depends_on, produces):
    """Clean the data (Python version)."""
    data = pd.read_pickle(depends_on)
    MonthlyPanel2 = monthlypanel_2(data)
    MonthlyPanel2.to_pickle(produces)


@pytask.mark.produces(BLD / "python" / "data" / "MonthlyPanel3.pkl")
@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
def task_monthlypanel_3(depends_on, produces):
    """Clean the data (Python version)."""
    data = pd.read_pickle(depends_on)
    MonthlyPanel3 = monthlypanel_3(data)
    MonthlyPanel3.to_pickle(produces)

@pytask.mark.produces(BLD / "python" / "data" / "MonthlyPanel_new.pkl")
@pytask.mark.depends_on(SRC / "data" / "MonthlyPanel.dta")
def task_monthlypanel_new(depends_on, produces):
    """Clean the data (Python version)."""
    data, meta = pyreadstat.read_dta(depends_on)
    MonthlyPanel_new = monthlypanel_new(data)
    MonthlyPanel_new.to_pickle(produces)