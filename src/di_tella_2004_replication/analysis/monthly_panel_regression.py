import numpy as np
import pandas as pd
import statsmodels.api as smm
from linearmodels.panel import PanelOLS

""""
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

"""


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
