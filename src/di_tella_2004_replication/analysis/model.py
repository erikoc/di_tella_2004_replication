import numpy as np
import pandas as pd
import scipy as scy
import statsmodels.api as smm
import statsmodels.formula.api as sm
from linearmodels.panel import PanelOLS
from scipy import stats


def WelchTest(Data, code1, code2):
    """Conducts a Welch test to compare the mean values of two specified variables in a
    dataset.

    Parameters
    ----------
    Data : pandas DataFrame
        A dataset containing the variables to be tested.
    code1 : str
        The first condition to be tested, specified as a string.
    code2 : str
        The second condition to be tested, specified as a string.

    Returns:
    -------
    t : float
        The calculated t-statistic value.
    p : float
        The calculated p-value.

    """
    WT = Data[
        ((Data["code"] == code1) | (Data["code"] == code2)) & (Data["month"] != 72)
    ]
    WT["code"].unique()
    code_1 = WT[WT["code"] == code1]
    code_2 = WT[WT["code"] == code2]
    cod_1 = code_1["total_thefts"].astype("int")
    cod_2 = code_2["total_thefts"].astype("int")
    t, p = stats.ttest_ind(cod_1, cod_2, equal_var=False)
    return t, p


def testings(regression, variable_test, testing_number):
    """Performs a t-test to determine if a given variable in a regression model is
    significantly different from a specified value.

    Parameters
    ----------
    regression : statsmodels regression results object
        The result object obtained from fitting a regression model.
    variable_test : str
        The name of the variable to test in the regression model.
    testing_number : float
        The value to test the variable against.

    Returns:
    -------
    Print string

    """
    tvalue = (regression.params[variable_test] - (testing_number)) / regression.bse[
        variable_test
    ]
    pvalue = 2 * (1 - scy.stats.t.cdf(np.abs(tvalue), regression.df_resid))
    if pvalue < 0.05:
        print(f"The coefficient of {variable_test}] is significantly different from {testing_number} with p-value", pvalue)
    else:
        print(f"The coefficient of {variable_test} is not significantly different from {testing_number} with p-value", pvalue)


def testings_div(regression, variable_test, testing_number, division_f):
    """Performs a t-test to determine if a given variable in a regression model is
    significantly different from a specified value.

    Parameters
    ----------
    regression : statsmodels regression results object
        The result object obtained from fitting a regression model.
    variable_test : str
        The name of the variable to test in the regression model.
    testing_number : float
        The value to test the variable against.

    Returns:
    -------
    Print string

    """
    tvalue = ((regression.params[variable_test] / division_f) - (testing_number)) / (
        regression.bse[variable_test] / division_f
    )
    pvalue = 2 * (1 - scy.stats.t.cdf(np.abs(tvalue), regression.df_resid))
    if pvalue < 0.05:
        print(f"The coefficient of {variable_test}] is significantly different from {testing_number} with p-value", pvalue)
    else:
        print(f"The coefficient of {variable_test} is not significantly different from {testing_number} with p-value", pvalue)


def areg_single(Data, variable_log, a, variable_fe, variable_y, variable_x):
    """Performs a fixed effects regression with clustered covariance.

    Parameters
    ----------
    Data : pandas DataFrame
        The input DataFrame containing the variables.
    variable_log : str
        The name of the variable to use for the logical condition.
    a : bool or int
        The value to use for the logical condition.
    variable_fe : str
        The name of the variable to use for the fixed effects.
    variable_y : str
        The name of the dependent variable in the regression.
    variable_x : list of str
        The names of the independent variables in the regression.

    Returns:
    -------
    statsmodels regression results object
        The result object obtained from fitting a fixed effects regression with clustered covariance.

    """
    m = Data[variable_log] == a
    fixed_effects = Data[variable_fe][m]
    y = Data[variable_y]
    x = Data[variable_x]
    X = smm.add_constant(x)
    reg = smm.OLS(y[m], X[m]).fit(
        cov_type="cluster",
        cov_kwds={"groups": fixed_effects},
    )
    return reg


def areg_double(
    Data,
    type_condition,
    variable_loga,
    variable_logb,
    a,
    variable_fe,
    variable_y,
    variable_x,
):
    """Performs a fixed effects regression with clustered covariance.

    Parameters:
        Data (pandas DataFrame): Dataset to perform the regression.
        type_condition (str): Type of condition to apply: 'equal' or 'unequal'.
        variable_loga (str): Name of the variable for condition a.
        variable_logb (str): Name of the variable for condition b.
        a (int): Value for the condition.
        variable_fe (str): Name of the variable to use as fixed effects for the clustered variance.
        variable_y (str): Name of the dependent variable.
        variable_x (str): Name of the independent variable.

    Returns:
        statsmodels.regression.linear_model.RegressionResultsWrapper: Results of the regression.

    Notes:
        This function performs a fixed effects regression with clustered covariance, using the provided dataset (Data), and two logical
        conditions (variable_loga, variable_logb) that should be met in the dataframe to perform the regression (depending on the
        value of the condition a). The fixed effects used for the clustered variance are given by variable_fe. The dependent variable
        is given by variable_y, and the independent variable is given by variable_x. Two types of regression can be performed depending
        on the value of type_condition: 'equal' or 'unequal'.

    """
    if type_condition == "equal":
        fixed_effects = Data.loc[
            (Data[variable_loga] == a) | (Data[variable_logb] == a)
        ]
        y = fixed_effects[variable_y]
        x = fixed_effects[variable_x]
        X = smm.add_constant(x)
        reg = smm.OLS(y, X).fit(
            cov_type="cluster",
            cov_kwds={"groups": fixed_effects[variable_fe]},
        )
        return reg
    elif type_condition == "unequal":
        fixed_effects = Data.loc[
            (Data[variable_loga] != a) | (Data[variable_logb] != a)
        ]
        y = fixed_effects[variable_y]
        x = fixed_effects[variable_x]
        X = smm.add_constant(x)
        reg = smm.OLS(y, X).fit(
            cov_type="cluster",
            cov_kwds={"groups": fixed_effects[variable_fe]},
        )
        return reg


def areg_triple(
    Data,
    variable_loga,
    variable_logb,
    variable_logc,
    a,
    variable_fe,
    variable_y,
    variable_x,
):
    """Perform a fixed effects regression with clustered covariance.

    Parameters:
    -----------
    Data : pandas DataFrame
        Data set to perform the regression on.
    variable_loga : str
        Name of variable in Data set used in condition 1.
    variable_logb : str
        Name of variable in Data set used in condition 2.
    variable_logc : str
        Name of variable in Data set used in condition 3.
    a : int or float
        Value to be used in the conditions.
    variable_fe : str
        Name of the variable in Data set to be used as fixed effects for the clustered variance.
    variable_y : str
        Name of the dependent variable in Data set.
    variable_x : str or list of str
        Name of the independent variable(s) in Data set.

    Returns:
    --------
    reg : statsmodels.regression.linear_model.RegressionResultsWrapper
        Results of the fixed effects regression with clustered covariance.

    """
    fixed_effects = Data.loc[
        (Data[variable_loga] == a)
        | (Data[variable_logb] == a)
        | (Data[variable_logc] == a)
    ]
    y = fixed_effects[variable_y]
    x = fixed_effects[variable_x]
    X = smm.add_constant(x)
    reg = smm.OLS(y, X).fit(
        cov_type="cluster",
        cov_kwds={"groups": fixed_effects[variable_fe]},
    )
    return reg


def reg_robust(Data, variable_y, variable_x):
    """Performs a robust regression using Huber's T norm, which gives less weight to
    outliers and more weight to inliers when calculating the scale of the data.

    Parameters:
    -----------
    Data : pandas.DataFrame
        The dataset containing the variables of interest.
    variable_y : str
        The name of the dependent variable.
    variable_x : str
        The name of the independent variable.

    Returns:
    --------
    reg : statsmodels.regression.linear_model.RLMResultsWrapper
        The results of the robust regression.

    """
    y = Data[variable_y]
    x = Data[variable_x]
    robust_model = smm.RLM(y, x, M=smm.robust.norms.HuberT())
    reg = robust_model.fit()
    return reg


def areg_clus(Data, variable_y, variable_x):
    """Perform a simple regression with cluster-robust standard errors.

    Args:
    - Data: pandas DataFrame containing the data to be used in the regression
    - variable_y: string representing the name of the dependent variable in Data
    - variable_x: string representing the name of the independent variable in Data

    Returns:
    - reg: a statsmodels regression result object containing the estimated parameters, standard errors,
    t-values, and other regression statistics. The covariance matrix used to calculate the standard errors
    is clustered by the variable 'observ' in Data.

    """
    y = Data[variable_y]
    x = Data[variable_x]
    X = smm.add_constant(x)
    reg = smm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": Data["observ"]})
    return reg


def areg_clus_abs(Data, drop_subset, y_variable, x_variable, dummy_variable):
    """Perform a simple regression with clusters on a dataset using a specified dummy
    variable for clustering.

    Args:
    - Data: DataFrame containing the dataset to be used for the regression.
    - drop_subset: A list of columns to be dropped from the DataFrame before running the regression.
    - y_variable: A string specifying the column name of the endogenous variable.
    - x_variable: A string specifying the column name of the exogenous variable.
    - dummy_variable: A string specifying the column name of the dummy variable to be used for clustering.

    Returns:
    - reg: A StatsModels OLS regression object containing the results of the regression.

    """
    df = Data.dropna(subset=drop_subset)
    y = df[y_variable]
    x = df[x_variable]
    dummies = pd.get_dummies(df[dummy_variable])
    X = pd.concat([x, dummies], axis=1)
    reg = smm.OLS(y, X).fit(
        cov_type="cluster",
        cov_kwds={"groups": df[dummy_variable]},
        use_t=True,
    )
    return reg


def poisson_reg(
    Data,
    y_variable,
    x_variable,
    index_variables,
    type_of_possion,
    weight,
    x_irra,
):
    """Performs a fixed effects poisson regression for different types of conditions
    reflected in (type_of_poisson); whether it is a simple fixed effects poisson
    regression, or is it weighted by a variable, in which case we input (weight) or is
    if we need to calculate interrater reliability (IRR) coefficients for a given set of
    data we add (x_irra).

    Args:
    - Data: pandas DataFrame containing the data
    - y_variable: string, name of the dependent variable in Data
    - x_variable: list of strings, names of the independent variables in Data
    - index_variables: list of strings, names of the index variables for panel data in Data
    - type_of_possion: string, type of Poisson regression to be performed. Options: 'fixed effects', 'fixed effects weighted',
      'fixed effects weighted irr'.
    - weight: string, name of the variable used as weights if the Poisson regression is weighted. Default is None.
    - x_irra: list of strings, names of the independent variables to calculate IRR if type_of_possion is 'fixed effects weighted irr'.
      Default is None.

    Returns:
    - reg: results of the Poisson regression
    - params: coefficients of the Poisson regression
    - predictions: predicted values of the dependent variable for the IRR calculation. Returned only if type_of_possion is 'fixed effects weighted irr'.
    - irr_predictions: incidence rate ratios (IRR) for the predicted values of the dependent variable. Returned only if
      type_of_possion is 'fixed effects weighted irr'.

    """
    data = Data.set_index(index_variables)
    Y = data[y_variable]
    X = data[x_variable]
    if type_of_possion == "fixed effects":
        model = PanelOLS(
            Y,
            X,
            entity_effects=True,
            time_effects=True,
            drop_absorbed=True,
        )
        reg = model.fit(cov_type="clustered", cluster_entity=True)
        params = reg.params
        return reg, params
    elif type_of_possion == "fixed effects weighted":
        w = Data[weight]
        data["iweight"] = w
        model = PanelOLS(
            Y,
            X,
            entity_effects=True,
            time_effects=True,
            drop_absorbed=True,
            weights=data["w"],
        )
        reg = model.fit(cov_type="clustered", cluster_entity=True)
        params = reg.params
        return reg, params
    elif type_of_possion == "fixed effects weighted irr":
        w = Data[weight]
        data["iweight"] = w
        model = PanelOLS(
            Y,
            X,
            entity_effects=True,
            time_effects=True,
            drop_absorbed=True,
            weights=data["w"],
        )
        reg = model.fit(cov_type="clustered", cluster_entity=True)
        params = reg.params
        predictions = reg.predict(X[x_irra])
        irr_predictions = np.exp(predictions)
        return reg, params, predictions, irr_predictions


# Calling the required dataframe
MonthlyPanel = pd.read_csv(
    "/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel.csv",
)

# table otromes1 if mes~=72, by(codigo) c(mean totrob2 sd totrob2);
MP = MonthlyPanel.apply(
    pd.to_numeric,
    errors="coerce",
)  # replacing non numeric values of totrob2 with NAs
MP.loc[MP["month"] != 72].groupby(["othermonth1", "code"])["total_thefts2"].agg(
    ["mean", "std"],
)

# by mes: ttest totrob2 if ((codigo==1 | codigo==4) & mes~=72), by (codigo) unequal welch; # THIS IS A WELCH TEST
code11 = 1
code21 = 4
Welch_Test1 = WelchTest(MonthlyPanel, code11, code21)

# by mes: ttest totrob2 if ((codigo==2 | codigo==4) & mes~=72), by (codigo) unequal welch;
code12 = 2
code22 = 4
Welch_Test2 = WelchTest(MonthlyPanel, code12, code22)

# by mes: ttest totrob2 if ((codigo==3 | codigo==4) & mes~=72), by (codigo) unequal welch;
code13 = 3
code23 = 4
Welch_Test3 = WelchTest(MonthlyPanel, code13, code23)

############################################## PART 2 ########################################################################################################################

# Calling the required dataframe
MonthlyPanel2 = pd.read_csv(
    "/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Checking_my_code/Clean_data/MonthlyPanel2.csv",
)

# sum totrob if post==1 & distanci>2;
MonthlyPanel2.loc[
    (MonthlyPanel2["post"] == 1) & (MonthlyPanel2["distance_to_jewish_inst"] > 2),
    "total_thefts",
].sum()

# Regressions

# reg totrob institu1 month* if post==1, robust;
formula1 = "total_thefts ~ jewish_inst"
formula1 = "+".join(
    [formula1] + [f"month{i}" for i in range(5, 13)],
)  # This is using a list comprehension
regression1 = sm.ols(formula1, data=MonthlyPanel2[MonthlyPanel2["post"] == 1]).fit()
# reg totrob institu1 inst3_1 month* if post==1, robust;
formula2 = "total_thefts ~ jewish_inst_one_block_away_1"
formula2 = "+".join(
    [formula2] + [f"month{i}" for i in range(5, 13)],
)  # This is using a list comprehension
regression2 = sm.ols(formula2, data=MonthlyPanel2[MonthlyPanel2["post"] == 1]).fit()
# reg totrob institu1 inst3_1 cuad2 month* if post==1, robust;
formula3 = "total_thefts ~ jewish_inst + jewish_inst_one_block_away_1 + cuad2"
formula3 = "+".join(
    [formula3] + [f"month{i}" for i in range(5, 13)],
)  # This is using a list comprehension
regression3 = sm.ols(formula3, data=MonthlyPanel2[MonthlyPanel2["post"] == 1]).fit()

# test institu1=-0.08080; # the hypothesis being tested is that the coefficient for "institu1" is equal to -0.08080
variable_test1 = "jewish_inst"
testing_number1 = -0.08080
test_diff1 = testings(regression1, variable_test1, testing_number1)

# test inst3_1=-0.01398;
variable_test2 = "jewish_inst_one_block_away_1"
testing_number2 = -0.01398
test_diff2 = testings(regression2, variable_test2, testing_number2)

# test cuad2=-0.00218;
variable_test3 = "cuad2"
testing_number3 = -0.00218
test_diff3 = testings(regression3, variable_test3, testing_number3)

# test institu1=-0.0543919;
variable_test11 = "jewish_inst"
testing_number11 = -0.0543919
test_diff11 = testings(regression1, variable_test11, testing_number11)

# test inst3_1=-0.0124224;
variable_test22 = "jewish_inst_one_block_away_1"
testing_number22 = -0.0124224
test_diff22 = testings(regression2, variable_test22, testing_number22)

# test cuad2=-0.0242257;
variable_test33 = "cuad2"
testing_number33 = -0.0242257
test_diff33 = testings(regression3, variable_test33, testing_number33)


# FIXED EFFECTS REGRESSIONS

# areg totrobc inst1p if institu1==1, absorb(observ) robust; # This is a linear regression with fixed effects
Data1 = MonthlyPanel2
variable_log_1 = "jewish_inst"
a1 = 1
variable_fe1 = "observ"
variable_y1 = "total_thefts_c"
variable_x1 = "jewish_inst_p"
regression_fe1 = areg_single(
    Data=Data1,
    variable_log=variable_log_1,
    a=a1,
    variable_fe=variable_fe1,
    variable_y=variable_y1,
    variable_x=variable_x1,
)

# areg totrobc inst1p inst3_1p if (institu1==1 | inst3_1==1), absorb(observ) robust;
Data2 = MonthlyPanel2
type_condition2 = "equal"
variable_loga_2 = "jewish_inst"
variable_logb_2 = "jewish_inst_one_block_away_1"
a2 = 1
variable_fe2 = "observ"
variable_y2 = "total_thefts_c"
variable_x2 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
regression_fe2 = areg_double(
    Data=Data2,
    type_condition=type_condition2,
    variable_loga=variable_loga_2,
    variable_logb=variable_logb_2,
    a=a2,
    variable_fe=variable_fe2,
    variable_y=variable_y2,
    variable_x=variable_x2,
)

# areg totrobc inst1p inst3_1p cuad2p if (institu1==1 | inst3_1==1 | cuad2==1), absorb(observ) robust;
# areg totrobc inst1p inst3_1p cuad2p if (institu1==1 | inst3_1==1 | cuad2==1), absorb(observ) robust;
Data3 = MonthlyPanel2
variable_loga_3 = "jewish_inst"
variable_logb_3 = "jewish_inst_one_block_away_1"
variable_logc_3 = "cuad2"
a3 = 1
variable_fe3 = "observ"
variable_y3 = "total_thefts_c"
variable_x3 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"]
regression_fe3 = areg_triple(
    Data=Data3,
    variable_loga=variable_loga_3,
    variable_logb=variable_logb_3,
    variable_logc=variable_logc_3,
    a=a3,
    variable_fe=variable_fe3,
    variable_y=variable_y3,
    variable_x=variable_x3,
)

# TESTS OVER FIXED EFFECTS

# test inst1p=-0.08080;
variable_test4 = "jewish_inst_p"
testing_number4 = -0.08080
test_diff4 = testings(regression_fe1, variable_test4, testing_number4)

# test inst3_1p=-0.01398;
variable_test5 = "jewish_inst_one_block_away_1_p"
testing_number5 = -0.01398
test_diff5 = testings(regression_fe2, variable_test5, testing_number5)

# test cuad2p=-0.00218;
variable_test6 = "cuad2p"
testing_number6 = -0.00218
test_diff6 = testings(regression_fe3, variable_test6, testing_number6)

# test inst1p=-0.0727188;
variable_test44 = "jewish_inst_p"
testing_number44 = -0.0727188
test_diff44 = testings(regression_fe1, variable_test44, testing_number44)

# test inst3_1p=-0.0115807;
variable_test55 = "jewish_inst_one_block_away_1_p"
testing_number55 = -0.0115807
test_diff55 = testings(regression_fe2, variable_test55, testing_number55)

# test cuad2p=-0.0034292;
variable_test66 = "cuad2p"
testing_number66 = -0.0034292
test_diff66 = testings(regression_fe3, variable_test66, testing_number66)

### Regressions clustered

# areg totrob inst1p month*, absorb(observ) robust;
Data_clu1 = MonthlyPanel2
y_clu1 = "total_thefts"
x_clu1 = [
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
regression_clu1 = areg_clus(Data_clu1, y_clu1, x_clu1)

# areg totrob inst1p inst3_1p month*, absorb(observ) robust;
Data_clu2 = MonthlyPanel2
y_clu2 = "total_thefts"
x_clu2 = [
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
regression_clu2 = areg_clus(Data_clu2, y_clu2, x_clu2)

# areg totrob inst1p inst3_1p cuad2p month*, absorb(observ) robust;
Data_clu3 = MonthlyPanel2
y_clu3 = "total_thefts"
x_clu3 = [
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
regression_clu3 = areg_clus(Data_clu3, y_clu3, x_clu3)

### TESTS

# test inst1p=-0.01221;
variable_test7 = "jewish_inst_p"
testing_number7 = -0.01221
test_diff7 = testings(regression_clu1, variable_test7, testing_number7)

variable_test8 = "jewish_inst_p"
testing_number8 = -regression_clu2.params["jewish_inst_one_block_away_1_p"]
division_f1 = 161 / 37
test_diff8 = testings_div(regression_clu2, variable_test8, testing_number8, division_f1)

variable_test9 = "jewish_inst_p"
testing_number9 = -regression_clu3.params["cuad2p"]
division_f2 = 226 / 37
test_diff9 = testings_div(regression_clu3, variable_test9, testing_number9, division_f2)

# test inst1p=-0.0727188;
variable_test77 = "jewish_inst_p"
testing_number77 = -0.0727188
test_diff77 = testings(regression_clu1, variable_test77, testing_number77)

# test inst3_1p=-0.0115807;
variable_test88 = "jewish_inst_one_block_away_1_p"
testing_number88 = -0.0115807
test_diff88 = testings(regression_clu2, variable_test88, testing_number88)

# test cuad2p=-0.0034292;
variable_test99 = "cuad2p"
testing_number99 = -0.0034292
test_diff99 = testings(regression_clu3, variable_test99, testing_number99)

# test inst1p=-0.0543919;
variable_test777 = "jewish_inst_p"
testing_number777 = -0.0543919
test_diff777 = testings(regression_clu1, variable_test777, testing_number777)

# test inst3_1p=-0.0124224;
variable_test888 = "jewish_inst_one_block_away_1_p"
testing_number888 = -0.0124224
test_diff888 = testings(regression_clu2, variable_test888, testing_number888)

# test cuad2p=-0.0242257;
variable_test999 = "cuad2p"
testing_number999 = -0.0242257
test_diff999 = testings(regression_clu3, variable_test999, testing_number999)

# reg totrob institu1 inst3_1 cuad2 inst1p inst3_1p cuad2p month*, robust;
Data_robust1 = MonthlyPanel2
variable_y_robust1 = "total_thefts"
variable_x_robust1 = [
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
]
reg_robust1 = reg_robust(
    Data=Data_robust1,
    variable_y=variable_y_robust1,
    variable_x=variable_x_robust1,
)

### MORE CLUSTERED REGRESSIONS


# areg robcoll inst1p inst3_1p cuad2p month*, absorb(observ) robust;
Data_clu4 = MonthlyPanel2
y_clu4 = "theftscoll"
x_clu4 = [
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
regression_clu4 = areg_clus(Data_clu4, y_clu4, x_clu4)

# areg totrob inst1p inst3_1p cuad2p month*, absorb(observ) robust cluster(observ);
Data_clus_abs1 = MonthlyPanel2
drop_subset_clus_abs1 = [
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
]
y_variable_clus_abs1 = "total_thefts"
x_variable_clus_abs1 = [
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
dummy_variable_clus_abs1 = "observ"
regression_clus_abs1 = areg_clus_abs(
    Data_clus_abs1,
    drop_subset_clus_abs1,
    y_variable_clus_abs1,
    x_variable_clus_abs1,
    dummy_variable_clus_abs1,
)

# summarize totrob if mes>7 & (institu1==0 & inst3_1==0 & cuad2==0 & (totpre~=0 | totpos~=0));
summary = MonthlyPanel2[
    (MonthlyPanel2["month"] > 7)
    & (MonthlyPanel2["jewish_inst"] == 0)
    & (MonthlyPanel2["jewish_inst_one_block_away_1"] == 0)
    & (MonthlyPanel2["cuad2"] == 0)
    & ((MonthlyPanel2["totalpre"] != 0) | (MonthlyPanel2["totalpos"] != 0))
]["total_thefts"].describe()
summary  # summary of totrob given the conditions above specified

# areg totrob inst1p inst3_1p cuad2p month* if (totpre~=0 | totpos~=0), absorb(observ) robust; # We can use fixed effects
Data4 = MonthlyPanel2
type_condition4 = "unequal"
variable_log_6 = "totalpre"
variable_log_7 = "totalpos"
a4 = 0
variable_fe4 = "observ"
variable_y4 = "total_thefts"
variable_x4 = [
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
regression_fe4 = areg_double(
    Data=Data4,
    type_condition=type_condition4,
    variable_loga=variable_log_6,
    variable_logb=variable_log_7,
    a=a4,
    variable_fe=variable_fe4,
    variable_y=variable_y4,
    variable_x=variable_x4,
)

# xtpois totrob inst1p inst3_1p cuad2p month*, fe i(observ);
Data_poisson1 = MonthlyPanel2
y_variable_poisson1 = "total_thefts"
x_variable_poission1 = [
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
index_variables_poisson1 = ["observ", "month"]
type_of_possion1 = "fixed effects"
weight_dumb = "total_thefts"  # not included as they are set out by default
x_irra_dumb = "total_thefts"  # not included as they are set out by default
reg_poisson1 = poisson_reg(
    Data=Data_poisson1,
    y_variable=y_variable_poisson1,
    x_variable=x_variable_poission1,
    index_variables=index_variables_poisson1,
    type_of_possion=type_of_possion1,
    weight=weight_dumb,
    x_irra=x_irra_dumb,
)

# xtpois totrobq inst1p inst3_1p cuad2p month* [iweight=w], fe i(observ);
Data_poisson2 = MonthlyPanel2
y_variable_poisson2 = "total_thefts"
x_variable_poission2 = [
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
index_variables_poisson2 = ["observ", "month"]
type_of_possion2 = "fixed effects weighted"
weight_poisson = "w"
x_irra_dumb = "total_thefts"  # not included as they are set out by default
reg_poisson2 = poisson_reg(
    Data=Data_poisson2,
    y_variable=y_variable_poisson2,
    x_variable=x_variable_poission2,
    index_variables=index_variables_poisson2,
    type_of_possion=type_of_possion2,
    weight=weight_poisson,
    x_irra=x_irra_dumb,
)

# xtpois totrobq inst1p inst3_1p cuad2p month* [iweight=w], fe i(observ) irr;
Data_poisson3 = MonthlyPanel2
y_variable_poisson3 = "total_thefts"
x_variable_poission3 = [
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
index_variables_poisson3 = ["observ", "month"]
type_of_possion3 = "fixed effects weighted irr"
weight_poisson2 = "w"
x_irra_poisson = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"]
reg_poisson3 = poisson_reg(
    Data=Data_poisson3,
    y_variable=y_variable_poisson3,
    x_variable=x_variable_poission3,
    index_variables=index_variables_poisson3,
    type_of_possion=type_of_possion3,
    weight=weight_poisson2,
    x_irra=x_irra_poisson,
)

# areg totrob inst1p inst3_1p cuad2p month*, absorb(observ) robust cluster(codigo2); # We can use the function areg_clus_abs
Data_clus_abs2 = MonthlyPanel2
drop_subset_clus_abs2 = [
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
]
y_variable_clus_abs2 = "total_thefts"
x_variable_clus_abs2 = [
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
dummy_variable_clus_abs2 = "code2"
regression_clus_abs2 = areg_clus_abs(
    Data_clus_abs2,
    drop_subset_clus_abs2,
    y_variable_clus_abs2,
    x_variable_clus_abs2,
    dummy_variable_clus_abs2,
)

# areg totrob inst1p inst3_1p cuad2p mbelg* monce* mvcre*, absorb(observ) robust;
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
Data_clu5 = MonthlyPanel2
y_clu5 = "total_thefts"
x_clu5 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"]
x_clu5.extend(list_names_place)
regression_clu5 = areg_clus(Data_clu5, y_clu5, x_clu5)

############################################## PART 3 ########################################################################################################################

# Calling the required dataframe
MonthlyPanel3 = pd.read_csv(
    "/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel3.csv",
)

# areg totrob nepin1p nepi3_1p nepcua2p epin1p epin3_1p epcuad2p month*, absorb(observ) robust; # USE areg_clus function
# areg totrob nesin1p nesi3_1p nescua2p esin1p esin3_1p escuad2p month*, absorb(observ) robust;
# areg totrob nbain1p nbai3_1p nbacua2p bain1p bain3_1p bacuad2p month*, absorb(observ) robust;
# areg totrob ntoin1p ntoi3_1p ntocua2p toin1p toin3_1p tocuad2p month*, absorb(observ) robust;

# Calling the necessary list
list_names_data3_0 = [
    "public_building_or_embassy_p",
    "public_building_or_embassy_1_p",
    "public_building_or_embassy_cuad2p",
    "n_public_building_or_embassy_p",
    "n_public_building_or_embassy_1_p",
    "n_public_building_or_embassy_cuad2p",
]
list_names_data3_1 = [
    "gas_station_p",
    "gas_station_1_p",
    "gas_station_cuad2p",
    "n_gas_station_p",
    "n_gas_station_1_p",
    "n_gas_station_cuad2p",
]
list_names_data3_2 = [
    "bank_p",
    "bank_1_p",
    "bank_cuad2p",
    "n_bank_p",
    "n_bank_1_p",
    "n_bank_cuad2p",
]
list_names_data3_3 = [
    "all_locations_p",
    "all_locations_1_p",
    "all_locations_cuad2p",
    "n_all_locations_p",
    "n_all_locations_1_p",
    "n_all_locations_cuad2p",
]


# Regression

# areg totrob nepin1p nepi3_1p nepcua2p epin1p epin3_1p epcuad2p month*, absorb(observ) robust;
Data_used = MonthlyPanel3
y_clu0 = "total_thefts"
x_clu0 = list_names_data3_0
reg_0_p3 = areg_clus(Data_used, y_clu0, x_clu0)
# areg totrob nesin1p nesi3_1p nescua2p esin1p esin3_1p escuad2p month*, absorb(observ) robust;
Data_used = MonthlyPanel3
y_clu1 = "total_thefts"
x_clu1 = list_names_data3_1
reg_1_p3 = areg_clus(Data_used, y_clu1, x_clu1)
# areg totrob nbain1p nbai3_1p nbacua2p bain1p bain3_1p bacuad2p month*, absorb(observ) robust;
Data_used = MonthlyPanel3
y_clu2 = "total_thefts"
x_clu2 = list_names_data3_2
reg_2_p3 = areg_clus(Data_used, y_clu2, x_clu2)
# areg totrob ntoin1p ntoi3_1p ntocua2p toin1p toin3_1p tocuad2p month*, absorb(observ) robust;
Data_used = MonthlyPanel3
y_clu3 = "total_thefts"
x_clu3 = list_names_data3_3
reg_3_p3 = areg_clus(Data_used, y_clu3, x_clu3)


### TESTS

# areg totrob nepin1p nepi3_1p nepcua2p epin1p epin3_1p epcuad2p month*, absorb(observ) robust;
tests0 = [
    (list_names_data3_0[0], list_names_data3_0[3]),
    (list_names_data3_0[1], list_names_data3_0[4]),
    (list_names_data3_0[2], list_names_data3_0[5]),
]

for var, test_num in tests0:
    test_res0 = testings(reg_0_p3, var, reg_0_p3.params[test_num])

# areg totrob nesin1p nesi3_1p nescua2p esin1p esin3_1p escuad2p month*, absorb(observ) robust;
tests1 = [
    (list_names_data3_1[0], list_names_data3_1[3]),
    (list_names_data3_1[1], list_names_data3_1[4]),
    (list_names_data3_1[2], list_names_data3_1[5]),
]

for var, test_num in tests1:
    test_res1 = testings(reg_1_p3, var, reg_1_p3.params[test_num])

# areg totrob nbain1p nbai3_1p nbacua2p bain1p bain3_1p bacuad2p month*, absorb(observ) robust;
tests2 = [
    (list_names_data3_2[0], list_names_data3_2[3]),
    (list_names_data3_2[1], list_names_data3_2[4]),
    (list_names_data3_2[2], list_names_data3_2[5]),
]

for var, test_num in tests2:
    test_res2 = testings(reg_2_p3, var, reg_2_p3.params[test_num])

# areg totrob ntoin1p ntoi3_1p ntocua2p toin1p toin3_1p tocuad2p month*, absorb(observ) robust;
tests3 = [
    (list_names_data3_3[0], list_names_data3_3[3]),
    (list_names_data3_3[1], list_names_data3_3[4]),
    (list_names_data3_3[2], list_names_data3_3[5]),
]

for var, test_num in tests3:
    test_res3 = testings(reg_3_p3, var, reg_3_p3.params[test_num])

############################################## PART 4 ########################################################################################################################

# Calling the required dataframe
MonthlyPanel_new = pd.read_csv(
    "/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel_new.csv",
)

# summarize totrob;
MonthlyPanel_new["total_thefts"].describe()

# areg totrob 1inst1p 1inst3_1p 1cuad2p month*, absorb(observ) robust; # USE areg_clus function
# areg totrob 2inst1p 2inst3_1p 2cuad2p month*, absorb(observ) robust;
# areg totrob 3inst1p 3inst3_1p 3cuad2p month*, absorb(observ) robust;

Data_clu_new = MonthlyPanel_new
y_clu_new = "total_thefts"
x_clu_new1 = [
    "one_jewish_inst_1_p",
    "one_jewish_inst_one_block_away_1_p",
    "one_cuad2p",
    "month5",
    "month6",
    "month7",
]
x_clu_new2 = [
    "two_jewish_inst_1_p",
    "two_jewish_inst_one_block_away_1_p",
    "two_cuad2p",
    "month5",
    "month6",
    "month7",
]
x_clu_new3 = [
    "three_jewish_inst_1_p",
    "three_jewish_inst_one_block_away_1_p",
    "three_cuad2p",
    "month5",
    "month6",
    "month7",
]

regression_new1 = areg_clus(Data_clu_new, y_clu_new, x_clu_new1)
regression_new2 = areg_clus(Data_clu_new, y_clu_new, x_clu_new2)
regression_new3 = areg_clus(Data_clu_new, y_clu_new, x_clu_new3)


"""WEEKLY"""

# Generate a function that will get us the regression results


def regression_WeeklyPanel(Data, y_variable, type_of_regression):
    """Performs a fixed effects regression on a weekly panel dataset.

    Args:
        Data (pd.DataFrame): The weekly panel dataset to use for the regression.
        y_variable (str): The name of the endogenous variable to use in the regression.
        type_of_regression (str): The type of regression to perform. Must be either "unclustered" or "clustered".

    Returns:
        pd.Series: The regression coefficients.

    Notes:
        This function creates a fixed effects regression using the "observ" variable as the fixed effect.
        If type_of_regression is "unclustered", the function returns the coefficients for an unclustered regression.
        If type_of_regression is "clustered", the function returns the coefficients for a clustered regression.

    """
    # Creating a list (using list comprehension) of the columns to be created
    list_names = ["week1"]
    list_names.extend([f"week{i}" for i in range(2, 40)])

    WeeklyP = Data[(Data["week"] != 16) & (Data["week"] != 17)]
    y = WeeklyP[y_variable]
    x = WeeklyP[
        ["jewish_inst_p", "jewish_int_one_block_away_1_p", "cuad2p"]
    ]  # inst1p = jewish_inst_p, inst3_1p = jewish_int_one_block_away_1_p
    x = list(x.columns) + list_names
    x_1 = WeeklyP[x]
    if type_of_regression == "unclustered":
        # Check if the modified MonthlyPanel_new is a pandas DataFrame
        X = smm.add_constant(x_1)
        reg = smm.OLS(y, X)
        result = reg.fit(
            cov_type="cluster",
            cov_kwds={"groups": WeeklyP["observ"]},
            hasconst=True,
        )
        params = result.params
        return params
    elif type_of_regression == "clustered":
        dummies = pd.get_dummies(
            WeeklyP["code2"],
        )  # to capture the fixed efefcts by codigo2
        X = pd.concat([x_1, dummies], axis=1)
        result = smm.OLS(y, X).fit(
            cov_type="cluster",
            cov_kwds={"groups": WeeklyP["observ"]},
            use_t=True,
        )  # with cluster for observ
        params = result.params
        return params


# Calling the required dataframe
WeeklyPanel = pd.read_csv(
    "/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Checking_my_code/Clean_data/WeeklyPanel.csv",
)

# summarize total_thefts;
WeeklyPanel["total_thefts"].describe()

# areg totrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust;
Data1 = WeeklyPanel
y_variable1 = "total_thefts"
type_of_regression1 = "unclustered"
regression_1 = regression_WeeklyPanel(Data1, y_variable1, type_of_regression1)

# areg totrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust cluster(codigo2);
Data2 = WeeklyPanel
y_variable2 = "total_thefts"
type_of_regression2 = "clustered"
regression_2 = regression_WeeklyPanel(Data2, y_variable2, type_of_regression2)

# areg ntotrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust cluster(codigo2);
Data3 = WeeklyPanel
y_variable3 = "n_total_thefts"
type_of_regression3 = "clustered"
regression_3 = regression_WeeklyPanel(Data3, y_variable3, type_of_regression3)
