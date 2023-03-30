"""Tasks running the results formatting (tables, figures)."""
import pickle

import pytask

import statsmodels.formula.api as sm

from di_tella_2004_replication.config import BLD


"Crime by Block"

@pytask.mark.depends_on(BLD / "python" / "models" / "fe_tot_models.pickle")
@pytask.mark.produces(
    {
        "hv": BLD / "python" / "tables" / "fe_tot_models_results_hv.tex",
        "lv": BLD / "python" / "tables" / "fe_tot_models_results_lv.tex",
        "night": BLD / "python" / "tables" / "fe_tot_models_results_night.tex",
        "day": BLD / "python" / "tables" / "fe_tot_models_results_day.tex",
        "weekday": BLD / "python" / "tables" / "fe_tot_models_results_weekday.tex",
        "weekend": BLD / "python" / "tables" / "fe_tot_models_results_weekend.tex",
    },
)
def task_create_results_fe_tot_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
    for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:
        table = model[suffix].summary.as_latex()
        with open(produces[suffix], "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "models" / "abs_tot_models.pickle")
@pytask.mark.produces(
    {
        "hv": BLD / "python" / "tables" / "abs_tot_models_results_hv.tex",
        "lv": BLD / "python" / "tables" / "abs_tot_models_results_lv.tex",
        "night": BLD / "python" / "tables" / "abs_tot_models_results_night.tex",
        "day": BLD / "python" / "tables" / "abs_tot_models_results_day.tex",
        "weekday": BLD / "python" / "tables" / "abs_tot_models_results_weekday.tex",
        "weekend": BLD / "python" / "tables" / "abs_tot_models_results_weekend.tex",
    },
)
def task_create_results_abs_tot_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
    for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:
        table = model[suffix].summary.as_latex()
        with open(produces[suffix], "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "models" / "fe_dif_models.pickle")
@pytask.mark.produces(
    {
        "hv_lv": BLD / "python" / "tables" / "fe_dif_models_results_hv_lv.tex",
        "night_day": BLD / "python" / "tables" / "fe_dif_models_results_night_day.tex",
        "weekday_weekend": BLD
        / "python"
        / "tables"
        / "fe_dif_models_results_weekday_weekend.tex",
    },
)
def task_create_results_fe_dif_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
    for suffix in ["hv_lv", "night_day", "weekday_weekend"]:
        table = model[suffix].summary.as_latex()
        with open(produces[suffix], "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "models" / "abs_dif_models.pickle")
@pytask.mark.produces(
    {
        "hv_lv": BLD / "python" / "tables" / "abs_dif_models_results_hv_lv.tex",
        "night_day": BLD / "python" / "tables" / "abs_dif_models_results_night_day.tex",
        "weekday_weekend": BLD
        / "python"
        / "tables"
        / "abs_dif_models_results_weekday_weekend.tex",
    },
)
def task_create_results_abs_dif_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
    for suffix in ["hv_lv", "night_day", "weekday_weekend"]:
        table = model[suffix].summary.as_latex()
        with open(produces[suffix], "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test_ind_char.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test_ind_char.tex")
def task_create_results_test_ind_char_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        tests.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "table_ind_char.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "table_ind_char.tex")
def task_create_results_group_ind_char_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        tests.to_latex(produces)



"WeeklyPanel"


"MonthlyPanel"

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_normal_regression1.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_normal_regression1.text")
def task_create_MonthlyPanel_normal_regression1(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)
            
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_normal_regression2.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_normal_regression2.text")
def task_create_MonthlyPanel_normal_regression2(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_normal_regression3.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_normal_regression3.text")
def task_create_MonthlyPanel_normal_regression3(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)



@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_single.text")
def task_create_MonthlyPanel_areg_single(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)
            
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_double.text")
def task_create_MonthlyPanel_areg_double(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_triple.text")
def task_create_MonthlyPanel_areg_triple(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)