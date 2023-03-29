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





"""WeeklyPanel"""

# for regressions #######################################################

from di_tella_2004_replication.analysis.weekly_panel_regression import (
    regression_WeeklyPanel
)
from di_tella_2004_replication.config import BLD

@pytask.mark.depends_on(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "Weekly_regression1.pickle")
def areg_weekly1(depends_on, produces):
    model = regression_WeeklyPanel(
        Data = pd.read_pickle(depends_on),
        y_variable = "total_thefts",
        type_of_regression = "unclustered"       
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "Weekly_regression2.pickle")
def areg_weekly2(depends_on, produces):
    model = regression_WeeklyPanel(
        Data = pd.read_pickle(depends_on),
        y_variable = "total_thefts",
        type_of_regression = "clustered"       
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "data" / "WeeklyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "Weekly_regression3.pickle")
def areg_weekly3(depends_on, produces):
    model = regression_WeeklyPanel(
        Data = pd.read_pickle(depends_on),
        y_variable = "n_total_thefts",
        type_of_regression = "clustered"       
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)





"""MonthlyPanel"""

# for regressions #################################################################
from di_tella_2004_replication.analysis.monthly_panel_regression import (
    areg_single,
    areg_double,
    areg_triple,
    reg_robust,
    areg_clus,
    areg_clus_abs,
    poisson_reg
)
from di_tella_2004_replication.config import BLD

@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
def areg_single_monthly(depends_on, produces):
    model =areg_single(
        Data = pd.read_pickle(depends_on),
        variable_log = "jewish_inst",
        a = 1,
        variable_fe = "observ",
        variable_y = "total_thefts_c",
        variable_x = "jewish_inst_p"      
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
def areg_double_monthly(depends_on, produces):
    model =areg_double(
        Data = pd.read_pickle(depends_on),
        type_condition = "equal",
        variable_loga = "jewish_inst",
        variable_logb = "jewish_inst_one_block_away_1",
        a = 1,
        variable_fe = "observ",
        variable_y = "total_thefts_c",
        variable_x = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
def areg_triple_monthly(depends_on, produces):
    model =areg_triple(
        Data = pd.read_pickle(depends_on),
        variable_loga = "jewish_inst",
        variable_logb = "jewish_inst_one_block_away_1",
        variable_logc = "cuad2",
        a = 1,
        variable_fe = "observ",
        variable_y = "total_thefts_c",
        variable_x = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"]   
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
        

@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
def areg_clus1_monthly(depends_on, produces):
    model =areg_clus(
        Data = pd.read_pickle(depends_on),
        variable_y = "total_thefts",
        variable_x = [
            "jewish_inst_p",
            "month5",
            "month6",
            "month7",
            "month8",
            "month9",
            "month10",
            "month11",
            "month12",
        ]     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
def areg_clus2_monthly(depends_on, produces):
    model =areg_clus(
        Data = pd.read_pickle(depends_on),
        variable_y = "total_thefts",
        variable_x = [
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
        ]     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel2.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
def areg_clus3_monthly(depends_on, produces):
    model =areg_clus(
        Data = pd.read_pickle(depends_on),
        variable_y = "total_thefts",
        variable_x = [
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
        ]     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)





# for stats ############################################################
from di_tella_2004_replication.analysis.monthly_panel_stats import (
    WelchTest,
    testings,
    testings_div
)

### WELCH TESTS first part

@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_WT1.pickle")
def WT_monthly1(depends_on, produces):
    model = WelchTest(
        Data = pd.read_pickle(depends_on),
        code1= 1,
        code2= 4       
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_WT2.pickle")
def WT_monthly2(depends_on, produces):
    model = WelchTest(
        Data = pd.read_pickle(depends_on),
        code1= 2,
        code2= 4       
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "data" / "MonthlyPanel.pkl")
@pytask.mark.produces(BLD / "python" / "models" / "MonthlyPanel_WT3.pickle")
def WT_monthly3(depends_on, produces):
    model = WelchTest(
        Data = pd.read_pickle(depends_on),
        code1= 3,
        code2= 4       
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


### Testings second areg simple, double, triple

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_single1_monthly.pickle")
def testings_areg1_single_Monthly(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -0.08080     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_single.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_single2_monthly.pickle")
def testings_areg2_single_Monthly(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -0.0727188     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_double1_monthly.pickle")
def testings_areg1_double_Monthly(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_one_block_away_1_p",
        testing_number = -0.01398     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_double.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_double2_monthly.pickle")
def testings_areg2_double_Monthly(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_one_block_away_1_p",
        testing_number = -0.0115807     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_triple1_monthly.pickle")
def testings_areg1_triple_Monthly(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "cuad2p",
        testing_number = -0.00218     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_triple.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test_areg_triple2_monthly.pickle")
def testings_areg2_triple_Monthly(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "cuad2p",
        testing_number = -0.0034292     
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)


### Testings second areg_clus

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test1_areg_clus1_monthly.pickle")
def testings1_areg_clus1(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -0.01221    
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test2_areg_clus1_monthly.pickle")
def testings2_areg_clus1(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -0.0727188   
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus1.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test3_areg_clus1_monthly.pickle")
def testings3_areg_clus1(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -0.0543919   
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
        
        
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test1_areg_clus2_monthly.pickle")
def testings1_areg_clus2(depends_on, produces):
    model = testings_div(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -(pd.read_pickle(depends_on).params["jewish_inst_one_block_away_1_p"]),
        division_f = 161 / 37    
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test2_areg_clus2_monthly.pickle")
def testings2_areg_clus2(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_one_block_away_1_p",
        testing_number = -0.0115807   
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus2.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test3_areg_clus2_monthly.pickle")
def testings3_areg_clus2(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -0.0124224   
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)




@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test1_areg_clus3_monthly.pickle")
def testings1_areg_clus3(depends_on, produces):
    model = testings_div(
        regression= pd.read_pickle(depends_on),
        variable_test = "jewish_inst_p",
        testing_number = -(pd.read_pickle(depends_on).params["cuad2p"]),
        division_f = 226 / 37    
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)
        
@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test2_areg_clus3_monthly.pickle")
def testings2_areg_clus3(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "cuad2p",
        testing_number = -0.0034292   
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)

@pytask.mark.depends_on(BLD / "python" / "models" / "MonthlyPanel_areg_clus3.pickle")
@pytask.mark.produces(BLD / "python" / "models" / "test3_areg_clus3_monthly.pickle")
def testings3_areg_clus3(depends_on, produces):
    model = testings(
        regression= pd.read_pickle(depends_on),
        variable_test = "cuad2p",
        testing_number = -0.0242257   
    )
    with open(produces, "wb") as f:
        pickle.dump(model, f)