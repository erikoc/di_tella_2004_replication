"""Tasks running the results formatting (tables, figures)."""
import pickle

import pandas as pd
import pytask

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


@pytask.mark.depends_on(BLD / "python" / "models" / "abs_reg_av_weekly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "abs_reg_weekly.pickle.tex")
def task_create_results_abs_reg_weekly_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary.as_latex()
        with open(produces, "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "models" / "abs_reg_weekly_clustered.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "abs_reg_weekly_clustered.pickle.tex")
def task_create_results_abs_reg_weekly_clustered_python(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary.as_latex()
        with open(produces, "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "models" / "abs_reg_weekly_robust.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "abs_reg_weekly_robust.pickle.tex")
def task_create_results__abs_reg_weekly_robust__python(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary.as_latex()
        with open(produces, "w") as f:
            f.writelines(table)


"MonthlyPanel"

# Regressions


@pytask.mark.depends_on(
    BLD / "python" / "models" / "MonthlyPanel_normal_regression1.pickle",
)
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_normal_regression1.tex")
def task_create_MonthlyPanel_normal_regression1(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(
    BLD / "python" / "models" / "MonthlyPanel_normal_regression2.pickle",
)
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_normal_regression2.tex")
def task_create_MonthlyPanel_normal_regression2(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(
    BLD / "python" / "models" / "MonthlyPanel_normal_regression3.pickle",
)
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_normal_regression3.tex")
def task_create_MonthlyPanel_normal_regression3(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_single.tex")
def task_create_MonthlyPanel_areg_single(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_double.tex")
def task_create_MonthlyPanel_areg_double(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_triple.tex")
def task_create_MonthlyPanel_areg_triple(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus1.tex")
def task_create_MonthlyPanel_areg_clus1(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus2.tex")
def task_create_MonthlyPanel_areg_clus2(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus3.tex")
def task_create_MonthlyPanel_areg_clus3(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_reg_robust1.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_reg_robust1.tex")
def task_create_MonthlyPanel_reg_robust1(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus4.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus4.tex")
def task_create_MonthlyPanel_areg_clus4(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(
    BLD / "python" / "models" / "MonthlyPanel_areg_clus_abs1.pickle",
)
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus_abs1.tex")
def task_create_MonthlyPanel_areg_clus_abs1(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_double2.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_double2.tex")
def task_create_MonthlyPanel_areg_double2(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_poisson1.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_poisson1.tex")
def task_create_MonthlyPanel_poisson1(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary.as_latex()
        with open(produces, "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_poisson2.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_poisson2.tex")
def task_create_MonthlyPanel_poisson2(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary.as_latex()
        with open(produces, "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_poisson3.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_poisson3.tex")
def task_create_MonthlyPanel_poisson3(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary.as_latex()
        with open(produces, "w") as f:
            f.writelines(table)


@pytask.mark.depends_on(
    BLD / "python" / "models" / "MonthlyPanel_areg_clus_abs2.pickle",
)
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus_abs2.tex")
def task_create_MonthlyPanel_areg_clus_abs2(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus5.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus5.tex")
def task_create_MonthlyPanel_areg_clus5(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus6.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus6.tex")
def task_create_MonthlyPanel_areg_clus6(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus7.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus7.tex")
def task_create_MonthlyPanel_areg_clus7(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus8.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus8.tex")
def task_create_MonthlyPanel_areg_clus8(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus9.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus9.tex")
def task_create_MonthlyPanel_areg_clus9(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus10.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus10.tex")
def task_create_MonthlyPanel_areg_clus10(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus11.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_areg_clus11.tex")
def task_create_MonthlyPanel_areg_clus11r(depends_on, produces):
    with open(depends_on, "rb") as f:
        model = pickle.load(f)
        table = model.summary2().tables[1]
        latex = table.to_latex()
        with open(produces, "w") as f:
            f.writelines(latex)


# Stats


@pytask.mark.depends_on(BLD / "python" / "stats" / "MonthlyPanel_WT1.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_WT1.tex")
def task_create_MonthlyPanel_WT1(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame({"t": [tests[0]], "p": [tests[1]]})
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "MonthlyPanel_WT2.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_WT2.tex")
def task_create_MonthlyPanel_WT2(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame({"t": [tests[0]], "p": [tests[1]]})
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "MonthlyPanel_WT3.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "MonthlyPanel_WT3.tex")
def task_create_MonthlyPanel_WT3(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame({"t-test": [tests[0]], "p-value": [tests[1]]})
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test_areg_single1_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test_areg_single1_monthly.tex")
def task_test_areg_single1_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test_areg_single2_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test_areg_single2_monthly.tex")
def task_test_areg_single2_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test_areg_double1_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test_areg_double1_monthly.tex")
def task_test_areg_double1_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test_areg_double2_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test_areg_double2_monthly.tex")
def task_test_areg_double1_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test_areg_triple1_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test_areg_triple1_monthly.tex")
def task_test_areg_triple1_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test_areg_triple2_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test_areg_triple2_monthly.tex")
def task_test_areg_triple2_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test1_areg_clus1_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test1_areg_clus1_monthly.tex")
def task_test1_areg_clus1_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test2_areg_clus1_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test2_areg_clus1_monthly.tex")
def task_test2_areg_clus1_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test3_areg_clus1_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test3_areg_clus1_monthly.tex")
def task_test3_areg_clus1_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test2_areg_clus2_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test2_areg_clus2_monthly.tex")
def task_test2_areg_clus2_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test3_areg_clus2_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test3_areg_clus2_monthly.tex")
def task_test3_areg_clus2_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test2_areg_clus3_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test2_areg_clus3_monthly.tex")
def task_test2_areg_clus3_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)


@pytask.mark.depends_on(BLD / "python" / "stats" / "test3_areg_clus3_monthly.pickle")
@pytask.mark.produces(BLD / "python" / "tables" / "test3_areg_clus3_monthly.tex")
def task_test3_areg_clus3_monthly(depends_on, produces):
    with open(depends_on, "rb") as f:
        tests = pickle.load(f)
        df = pd.DataFrame(
            {"t-test": [tests[0]], "p-value": [tests[1]], "sentence": [tests[2]]},
        )
        df.to_latex(produces)
