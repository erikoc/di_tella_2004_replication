"""Tasks running the core analyses."""
import pickle

import pandas as pd
import pytask

"""Crime by Block"""

from di_tella_2004_replication.analysis.crime_by_block_regression import (
    abs_regression_models_dif,
    abs_regression_models_totals,
    fe_regression_models_dif,
    fe_regression_models_totals,
)
from di_tella_2004_replication.analysis.crime_by_block_stats import (
    neighborhood_comparison_tables,
    t_tests_crime_by_block,
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


@pytask.mark.depends_on(BLD / "python" / "data" / "CrimeByBlockIndChar.pkl")
@pytask.mark.produces(BLD / "python" / "stats" / "test_ind_char.pickle")
def task_test_ind_chat_python(depends_on, produces):
    data = pd.read_pickle(depends_on)
    test = t_tests_crime_by_block(data)
    with open(produces, "wb") as t:
        pickle.dump(test, t)


@pytask.mark.depends_on(BLD / "python" / "data" / "CrimeByBlockIndChar.pkl")
@pytask.mark.produces(BLD / "python" / "stats" / "table_ind_char.pickle")
def task_comparison_table_ind_chat_python(depends_on, produces):
    data = pd.read_pickle(depends_on)
    comparison_table = neighborhood_comparison_tables(data)
    comparison_table.to_pickle(produces)


"""WeeklyPanel"""

# for regressions #######################################################

from di_tella_2004_replication.analysis.weekly_panel_regression import (
    abs_regression_models_av_weekly,
    abs_regression_models_weekly,
)


@pytask.mark.depends_on(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "stats" / "abs_reg_weekly_clustered.pickle")
def task_abs_reg_weekly_clustered(depends_on, produces):
    data = pd.read_pickle(depends_on)
    model = abs_regression_models_weekly(data, "clustered")
    with open(produces, "wb") as m:
        pickle.dump(model, m)


@pytask.mark.depends_on(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "stats" / "abs_reg_weekly_robust.pickle")
def task_abs_reg_weekly_robust(depends_on, produces):
    data = pd.read_pickle(depends_on)
    model = abs_regression_models_weekly(data, "robust")
    with open(produces, "wb") as m:
        pickle.dump(model, m)


@pytask.mark.depends_on(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "stats" / "abs_reg_av_weekly.pickle")
def task_abs_reg_av_weekly(depends_on, produces):
    data = pd.read_pickle(depends_on)
    model = abs_regression_models_av_weekly(data)
    with open(produces, "wb") as m:
        pickle.dump(model, m)


"""MonthlyPanel"""

# for regressions #################################################################
from di_tella_2004_replication.analysis.monthly_panel_regression import (
    areg_clus,
    areg_clus_abs,
    areg_double,
    areg_single,
    areg_triple,
    normal_regression,
    poisson_reg,
    reg_robust,
)
from di_tella_2004_replication.config import BLD


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(
    BLD / "python" / "models" / "MonthlyPanel_normal_regression1.pickle",
)
def task_normal_regression1_monthly(depends_on, produces):
    model = normal_regression(
        Data=pd.read_pickle(depends_on)[pd.read_pickle(depends_on)["post"] == 1],
        formula="total_thefts ~ jewish_inst",
        columns=[f"month{i}" for i in range(5, 13)],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(
    BLD / "python" / "models" / "MonthlyPanel_normal_regression2.pickle",
)
def task_normal_regression2_monthly(depends_on, produces):
    model = normal_regression(
        Data=pd.read_pickle(depends_on)[pd.read_pickle(depends_on)["post"] == 1],
        formula="total_thefts ~ jewish_inst + jewish_inst_one_block_away_1",
        columns=[f"month{i}" for i in range(5, 13)],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(
    BLD / "python" / "models" / "MonthlyPanel_normal_regression3.pickle",
)
def task_normal_regression3_monthly(depends_on, produces):
    model = normal_regression(
        Data=pd.read_pickle(depends_on)[pd.read_pickle(depends_on)["post"] == 1],
        formula="total_thefts ~ jewish_inst + jewish_inst_one_block_away_1 + cuad2",
        columns=[f"month{i}" for i in range(5, 13)],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
def task_areg_single_monthly(depends_on, produces):
    model = areg_single(
        Data=pd.read_pickle(depends_on),
        variable_log="jewish_inst",
        a=1,
        variable_fe="observ",
        variable_y="total_thefts_c",
        variable_x="jewish_inst_p",
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
def task_areg_double_monthly(depends_on, produces):
    model = areg_double(
        Data=pd.read_pickle(depends_on),
        type_condition="equal",
        variable_loga="jewish_inst",
        variable_logb="jewish_inst_one_block_away_1",
        a=1,
        variable_fe="observ",
        variable_y="total_thefts_c",
        variable_x=["jewish_inst_p", "jewish_inst_one_block_away_1_p"],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
def task_areg_triple_monthly(depends_on, produces):
    model = areg_triple(
        Data=pd.read_pickle(depends_on),
        variable_loga="jewish_inst",
        variable_logb="jewish_inst_one_block_away_1",
        variable_logc="cuad2",
        a=1,
        variable_fe="observ",
        variable_y="total_thefts_c",
        variable_x=["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
def task_areg_clus1_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "jewish_inst_p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
def task_areg_clus2_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
def task_areg_clus3_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_reg_robust1.pickle")
def task_reg_robust1_monthly(depends_on, produces):
    model = reg_robust(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "jewish_inst",
            "jewish_inst_one_block_away_1",
            "cuad2",
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus4.pickle")
def task_areg_clus4_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts2",
        variable_x=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus_abs1.pickle")
def task_areg_clus_abs1_monthly(depends_on, produces):
    model = areg_clus_abs(
        Data=pd.read_pickle(depends_on),
        drop_subset=[
            "total_thefts",
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
        y_variable="total_thefts",
        x_variable=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
        dummy_variable="observ",
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_double2.pickle")
def task_areg_double2_monthly(depends_on, produces):
    model = areg_double(
        Data=pd.read_pickle(depends_on),
        type_condition="unequal",
        variable_loga="totalpre",
        variable_logb="totalpos",
        a=0,
        variable_fe="observ",
        variable_y="total_thefts",
        variable_x=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_poisson1.pickle")
def task_poisson1_monthly(depends_on, produces):
    model = poisson_reg(
        Data=pd.read_pickle(depends_on),
        y_variable="total_thefts",
        x_variable=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
        index_variables=["observ", "month"],
        type_of_possion="fixed effects",
        weight=None,
        x_irra=None,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_poisson2.pickle")
def task_poisson2_monthly(depends_on, produces):
    model = poisson_reg(
        Data=pd.read_pickle(depends_on),
        y_variable="total_thefts",
        x_variable=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
        index_variables=["observ", "month"],
        type_of_possion="fixed effects",
        weight="w",
        x_irra=None,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_poisson3.pickle")
def task_poisson3_monthly(depends_on, produces):
    model = poisson_reg(
        Data=pd.read_pickle(depends_on),
        y_variable="total_thefts",
        x_variable=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
        index_variables=["observ", "month"],
        type_of_possion="fixed effects",
        weight="w",
        x_irra=["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus_abs2.pickle")
def task_areg_clus_abs2_monthly(depends_on, produces):
    model = areg_clus_abs(
        Data=pd.read_pickle(depends_on),
        drop_subset=[
            "total_thefts",
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
        y_variable="total_thefts",
        x_variable=[
            "jewish_inst_p",
            "jewish_inst_one_block_away_1_p",
            "cuad2p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ],
        dummy_variable="code2",
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus5.pickle")
def task_areg_clus5_monthly(depends_on, produces):
    list_names_place = []
    list_names_place.extend(
        [
            f"mbelg{i}"
            for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]
        ],
    )
    list_names_place.extend(
        [
            f"monce{i}"
            for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]
        ],
    )
    list_names_place.extend(
        [
            f"mvcre{i}"
            for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]
        ],
    )
    variablex = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"]
    variablex.extend(list_names_place)
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=variablex,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel3.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus5.pickle")
def task_areg_clus5_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "public_building_or_embassy_p",
            "public_building_or_embassy_1_p",
            "public_building_or_embassy_cuad2p",
            "n_public_building_or_embassy_p",
            "n_public_building_or_embassy_1_p",
            "n_public_building_or_embassy_cuad2p",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel3.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus6.pickle")
def task_areg_clus6_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "gas_station_p",
            "gas_station_1_p",
            "gas_station_cuad2p",
            "n_gas_station_p",
            "n_gas_station_1_p",
            "n_gas_station_cuad2p",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel3.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus7.pickle")
def task_areg_clus7_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "bank_p",
            "bank_1_p",
            "bank_cuad2p",
            "n_bank_p",
            "n_bank_1_p",
            "n_bank_cuad2p",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel3.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus8.pickle")
def task_areg_clus8_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "all_locations_p",
            "all_locations_1_p",
            "all_locations_cuad2p",
            "n_all_locations_p",
            "n_all_locations_1_p",
            "n_all_locations_cuad2p",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel_new.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus9.pickle")
def task_areg_clus9_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "one_jewish_inst_1_p",
            "one_jewish_inst_one_block_away_1_p",
            "one_cuad2p",
            "month5",
            "month6",
            "month7",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel_new.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus10.pickle")
def task_areg_clus10_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "two_jewish_inst_1_p",
            "two_jewish_inst_one_block_away_1_p",
            "two_cuad2p",
            "month5",
            "month6",
            "month7",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel_new.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus11.pickle")
def task_areg_clus11_monthly(depends_on, produces):
    model = areg_clus(
        Data=pd.read_pickle(depends_on),
        variable_y="total_thefts",
        variable_x=[
            "three_jewish_inst_1_p",
            "three_jewish_inst_one_block_away_1_p",
            "three_cuad2p",
            "month5",
            "month6",
            "month7",
        ],
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


# for stats ############################################################
from di_tella_2004_replication.analysis.monthly_panel_stats import (
    WelchTest,
    regression_testing,
    summarize_data,
    testings_div,
    various_testings,
)

### WELCH TESTS first part


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_WT1.pickle")
def task_WT_monthly1(depends_on, produces):
    model = WelchTest(Data=pd.read_pickle(depends_on), code1=1, code2=4)
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_WT2.pickle")
def task_WT_monthly2(depends_on, produces):
    model = WelchTest(Data=pd.read_pickle(depends_on), code1=2, code2=4)
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_WT3.pickle")
def task_WT_monthly3(depends_on, produces):
    model = WelchTest(Data=pd.read_pickle(depends_on), code1=3, code2=4)
    with open(produces, "wb") as f:
        pickle.dump(model, f)


### Testings second areg simple, double, triple


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_single1_monthly.pickle")
def task_testings_areg1_single_Monthly(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-0.08080,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_single2_monthly.pickle")
def task_testings_areg2_single_Monthly(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-0.0727188,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_double1_monthly.pickle")
def task_testings_areg1_double_Monthly(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_one_block_away_1_p",
        testing_number=-0.01398,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_double2_monthly.pickle")
def task_testings_areg2_double_Monthly(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_one_block_away_1_p",
        testing_number=-0.0115807,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_triple1_monthly.pickle")
def task_testings_areg1_triple_Monthly(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="cuad2p",
        testing_number=-0.00218,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_triple2_monthly.pickle")
def task_testings_areg2_triple_Monthly(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="cuad2p",
        testing_number=-0.0034292,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


### Testings second areg_clus


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test1_areg_clus1_monthly.pickle")
def task_testings1_areg_clus1(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-0.01221,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test2_areg_clus1_monthly.pickle")
def task_testings2_areg_clus1(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-0.0727188,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test3_areg_clus1_monthly.pickle")
def task_testings3_areg_clus1(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-0.0543919,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test1_areg_clus2_monthly.pickle")
def task_testings1_areg_clus2(depends_on, produces):
    model = testings_div(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-(
            pd.read_pickle(depends_on).params["jewish_inst_one_block_away_1_p"]
        ),
        division_f=161 / 37,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test2_areg_clus2_monthly.pickle")
def task_testings2_areg_clus2(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_one_block_away_1_p",
        testing_number=-0.0115807,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test3_areg_clus2_monthly.pickle")
def task_testings3_areg_clus2(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-0.0124224,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test1_areg_clus3_monthly.pickle")
def task_testings1_areg_clus3(depends_on, produces):
    model = testings_div(
        regression=pd.read_pickle(depends_on),
        variable_test="jewish_inst_p",
        testing_number=-(pd.read_pickle(depends_on).params["cuad2p"]),
        division_f=226 / 37,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test2_areg_clus3_monthly.pickle")
def task_testings2_areg_clus3(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="cuad2p",
        testing_number=-0.0034292,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test3_areg_clus3_monthly.pickle")
def task_testings3_areg_clus3(depends_on, produces):
    model = regression_testing(
        regression=pd.read_pickle(depends_on),
        variable_test="cuad2p",
        testing_number=-0.0242257,
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "Summary_of_data.pickle")
def task_summary_of_data(depends_on, produces):
    model = summarize_data(
        df=pd.read_pickle(depends_on),
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus5.pickle")
@pytask.mark.produces(
    BLD / "python" / "models" / "joint_tests_areg_clus5_monthly.pickle",
)
def task_joint_tests_areg_clus5(depends_on, produces):
    list_names_data3 = [
        "public_building_or_embassy_p",
        "public_building_or_embassy_1_p",
        "public_building_or_embassy_cuad2p",
        "n_public_building_or_embassy_p",
        "n_public_building_or_embassy_1_p",
        "n_public_building_or_embassy_cuad2p",
    ]
    model = various_testings(
        list_names_data=list_names_data3,
        reg=pd.read_pickle(depends_on),
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus6.pickle")
@pytask.mark.produces(
    BLD / "python" / "models" / "joint_tests_areg_clus6_monthly.pickle",
)
def task_joint_tests_areg_clus6(depends_on, produces):
    list_names_data3 = [
        "gas_station_p",
        "gas_station_1_p",
        "gas_station_cuad2p",
        "n_gas_station_p",
        "n_gas_station_1_p",
        "n_gas_station_cuad2p",
    ]
    model = various_testings(
        list_names_data=list_names_data3,
        reg=pd.read_pickle(depends_on),
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus7.pickle")
@pytask.mark.produces(
    BLD / "python" / "models" / "joint_tests_areg_clus7_monthly.pickle",
)
def task_joint_tests_areg_clus7(depends_on, produces):
    list_names_data3 = [
        "bank_p",
        "bank_1_p",
        "bank_cuad2p",
        "n_bank_p",
        "n_bank_1_p",
        "n_bank_cuad2p",
    ]
    model = various_testings(
        list_names_data=list_names_data3,
        reg=pd.read_pickle(depends_on),
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus8.pickle")
@pytask.mark.produces(
    BLD / "python" / "models" / "joint_tests_areg_clus8_monthly.pickle",
)
def task_joint_tests_areg_clus8(depends_on, produces):
    list_names_data3 = [
        "all_locations_p",
        "all_locations_1_p",
        "all_locations_cuad2p",
        "n_all_locations_p",
        "n_all_locations_1_p",
        "n_all_locations_cuad2p",
    ]
    model = various_testings(
        list_names_data=list_names_data3,
        reg=pd.read_pickle(depends_on),
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
