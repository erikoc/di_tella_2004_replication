import numpy as np
import pandas as pd
import pytest

import numpy as np
import pandas as pd
import scipy as scy
import statsmodels.api as smm
import statsmodels.formula.api as sm
from linearmodels.panel import PanelOLS
from scipy import stats

from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.analysis.model import (
    regression_WeeklyPanel # for WeeklyPanel   
)

"""""
from di_tella_2004_replication.analysis.model import fit_logit_model
"""""


"""""

DESIRED_PRECISION = 10e-2


@pytest.fixture()
def data():
    np.random.seed(0)
    x = np.random.normal(size=100_000)
    coef = 2.0
    prob = 1 / (1 + np.exp(-coef * x))
    return pd.DataFrame(
        {"outcome_numerical": np.random.binomial(1, prob), "covariate": x},
    )


@pytest.fixture()
def data_info():
    return {"outcome": "outcome", "outcome_numerical": "outcome_numerical"}


def test_fit_logit_model_recover_coefficients(data, data_info):
    model = fit_logit_model(data, data_info, model_type="linear")
    params = model.params
    assert np.abs(params["Intercept"]) < DESIRED_PRECISION
    assert np.abs(params["covariate"] - 2.0) < DESIRED_PRECISION


def test_fit_logit_model_error_model_type(data, data_info):
    with pytest.raises(ValueError):  # noqa: PT011
        assert fit_logit_model(data, data_info, model_type="quadratic")
        

"""""


"""WEEKLY PANEL"""

"Fixtures"
        
@pytest.fixture
def input_data_regression_WeeklyPanel():
    return pd.DataFrame({
        'week': [1, 2, 3, 4],
        'jewish_inst_p': [1, 2, 3, 4],
        'jewish_int_one_block_away_1_p': [2, 3, 4, 5],
        'cuad2p': [3, 4, 5, 6],
        'observ': [1, 1, 2, 2],
        'code2': [1, 1, 2, 2],
        'y_variable': [10, 20, 30, 40]
    }) # using a reference DataFrame because the function is more complex to grasp
    
    

"Tests"

def test_regression_WeeklyPanel(input_data_regression_WeeklyPanel):
    params = regression_WeeklyPanel(input_data_regression_WeeklyPanel, 'y_variable', 'clustered')
    assert len(params) == 46 # There are 43 weeks plus the 3 fixed effects variables
    assert params[0] == pytest.approx(2.238, abs=1e-3) # Check first parameter with tolerance
    
 
 
 
    
    

"""MONTHLY PANEL"""
    
# from model import WelchTest
    
# Defining WelchTest function
def WelchTest(Data, code1, code2):
    
    """ This is a Welch test which is trying to compare the equality of two values.
    What we have is simply a data set in which test that fits two conditions and these condition
    are being reflectes in code1 and code 2. We also have a Data set in which the test is made
    The will check whether the mean values of two specified variables are statitically different or not.
    This is reflected and assesed via the t-statistic and the p-value"""
    
    WT = Data[((Data["code"] == code1) | (Data["code"] == code2)) & (Data["month"] != 72)]
    codigo_values = WT["code"].unique()
    code_1 = WT[WT["code"] == code1]
    code_2 = WT[WT["code"] == code2]
    cod_1 = code_1["total_thefts"].astype('int')
    cod_2 = code_2["total_thefts"].astype('int')
    t, p = stats.ttest_ind(cod_1, cod_2, equal_var=False)
    print("code: ", [code1,  code2])
    print("t-statistic: ", t)
    print("p-value: ", p)

@pytest.fixture
def input_data_WelchTest():   
    Data = pd.DataFrame({'code': [1, 1, 2, 2],
                         'month': [1, 2, 1, 2],
                         'total_thefts': [10, 12, 8, 9]})
    code1 = 1
    code2 = 2
    return Data, code1, code2

def test_WelchTest(input_data_WelchTest): 
    Data, code1, code2 = input_data_WelchTest
    # run the WelchTest 
    t, p = WelchTest(Data, code1, code2)
    # we run a Welch test before hand with our above parameters and we get the following expected values
    expected_t = 1.404
    expected_p = 0.243
    # check if the calculated t and p values match the our before hand calculated values
    assert round(t, 3) == expected_t
    assert round(p, 3) == expected_p




# from model import testings

# Defining Regression Test Function

def testings(regression, variable_test, testing_number):
    
    """" What this function is doing is just to perform a t-statistic test with the coefficients of a regression (regression) in which it is checked 
    whether a variable (variable_test) is statistically close in value to a certain fixed value (testing_number) by us (in this case
    by the authors of the paper)"""
    
    tvalue = (regression.params[variable_test] - (testing_number)) / regression.bse[variable_test]
    pvalue = 2 * (1 - scy.stats.t.cdf(np.abs(tvalue), regression.df_resid))
    if pvalue < 0.05:
        print("The coefficient for the variable is significantly different from null hypothesis value with p-value", pvalue)
    else:
        print("The coefficient for the variable is not significantly different from null hypothesis value with p-value", pvalue)
        
@pytest.fixture
def input_data_testings():
    data = {'var_y': [1, 2, 3, 4, 5],
            'var_x': [10, 20, 30, 40, 50],
            'fixed_effects': [1, 1, 2, 2, 3],
            'variable_log': [1, 1, 1, 0, 0]}
    df = pd.DataFrame(data)
    return df

def test_testings_areg_single(input_data_testings): # We use the testing applied to the areg_single regression below tested
    result = areg_single(Data=input_data_testings, variable_log='variable_log', a=1, variable_fe='fixed_effects', variable_y='var_y', variable_x='var_x') # example using the fixed effects regression
    assert result.params[1] == 1.0 # precalculated value for x coefficient
    assert result.params[0] == -1.0 # pre calculated value for the intercept

# Defining Regression Test Function with division factors

def testings_div(regression, variable_test, testing_number, division_f):
    
    """" What this function is doing is just to perform a t-statistic test with the coefficients of a regression (regression) in which it is checked 
    whether a variable (variable_test) is statistically close in value to a certain fixed value (testing_number) by us (in this case
    by the authors of the paper)"""
    
    tvalue = ((regression.params[variable_test]/division_f) - (testing_number)) / (regression.bse[variable_test]/division_f)
    pvalue = 2 * (1 - scy.stats.t.cdf(np.abs(tvalue), regression.df_resid))
    if pvalue < 0.05:
        print("The coefficient for the variable is significantly different from null hypothesis value with p-value", pvalue)
    else:
        print("The coefficient for the variable is not significantly different from null hypothesis value with p-value", pvalue)
        
        
 ### Just one test as it is basically the same function
 
 
 
        
        

# Defing areg, FIXED EFFECTS AND CLUSTERS
    
def areg_single(Data, variable_log, a, variable_fe, variable_y, variable_x):
    """" What this function is doing is performing a fixed effects regression with clustered covariance of these. 
    It requires a variable (variable_fe) that will be used as the fixed effects for the clustered variance. It uses a Data set (Data) in order to perform 
    the regression. And it uses condtions thats should be met in the dataframe (variable_log, a). It also needs the input variables (variable_y, variable_x)"""""
    m = Data[variable_log] == a # the logical condition that must be accomplished to do the regression in the data
    fixed_effects = Data[variable_fe][m] # observ is used for our fixed effects only for those lines where institu1==1
    y = Data[variable_y]
    x = Data[variable_x]
    X = smm.add_constant(x) # adding a constant to the Xs
    reg = smm.OLS(y[m], X[m]).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
    #cov_kwds={'groups': fixed_effects} argument specifies the group variable to use in computing the cluster-robust standard errors
    params = reg.params
    return reg
    

def areg_double(Data, type_condition, variable_loga, variable_logb, a, variable_fe, variable_y, variable_x):
    """" What this function is doing is performing a fixed effects regression with clustered covariance of these. 
    It requires a variable (variable_fe) that will be used as the fixed effects for the clustered variance. It uses a Data set (Data) in order to perform 
    the regression. And it uses condtions thats should be met in the dataframe (variable_loga, variable_lob, a). 
    It also needs the input variables (variable_y, variable_x). It can perform 2 types of regression given a condition of equality or inequality (type_condition)"""""
    if type_condition == "equal":
        fixed_effects = Data.loc[(Data[variable_loga] == a) | (Data[variable_logb] == a)]
        y = fixed_effects[variable_y]
        x = fixed_effects[variable_x]
        X = smm.add_constant(x) # adding a constant to the Xs
        reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects[variable_fe]}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
        # cov_kwds={'groups': usage['observ']} argument specifies the group variable to use in computing the cluster-robust standard errors
        params = reg.params
        return reg
    elif type_condition == "unequal":
        fixed_effects = Data.loc[(Data[variable_loga] != a) | (Data[variable_logb] != a)]
        y = fixed_effects[variable_y]
        x = fixed_effects[variable_x]
        X = smm.add_constant(x) # adding a constant to the Xs
        reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects[variable_fe]}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
        # cov_kwds={'groups': usage['observ']} argument specifies the group variable to use in computing the cluster-robust standard errors
        params = reg.params
        return reg
 
 # from model import areg_triple   

def areg_triple(Data, variable_loga, variable_logb, variable_logc, a, variable_fe, variable_y, variable_x):
    """" What this function is doing is performing a fixed effects regression with clustered covariance of these. 
    It requires a variable (variable_fe) that will be used as the fixed effects for the clustered variance. It uses a Data set (Data) in order to perform 
    the regression. And it uses condtions thats should be met in the dataframe (variable_loga, variable_logb, variable_loc, a). 
    It also needs the input variables (variable_y, variable_x)"""""
    fixed_effects = Data.loc[(Data[variable_loga] == a) | (Data[variable_logb] == a) | (Data[variable_logc] == a)]
    y = fixed_effects[variable_y]
    x = fixed_effects[variable_x]
    X = smm.add_constant(x) # adding a constant to the Xs
    reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects[variable_fe]}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
# cov_kwds={'groups': usage['observ']} argument specifies the group variable to use in computing the cluster-robust standard errors
    params = reg.params
    return reg

@pytest.fixture
def input_data_areg_triple():
    Data = pd.DataFrame({ # The variables in question only have 0 and 1 values except for y and x 
        'loga': [1, 0, 0, 1, 0],
        'logb': [0, 1, 1, 0, 0],
        'logc': [0, 0, 0, 1, 1],
        'fe': [1, 0, 1, 0, 1],
        'y': [1, 2, 3, 4, 5],
        'x': [6, 7, 8, 9, 10]
    })
    variable_loga = Data['loga']
    variable_logb = Data['logb']
    variable_logc = Data['logc']
    a = 1
    variable_fe = Data['fe']
    variable_y = Data['y']
    variable_x = Data['x']
    return Data, variable_loga, variable_logb, variable_logc, a, variable_fe, variable_y, variable_x

def test_areg_triple(input_data_areg_triple): 
    Data, variable_loga, variable_logb, variable_logc, a, variable_fe, variable_y, variable_x = input_data_areg_triple
    
    # Implementing the regression
    result = areg_triple(Data, variable_loga, variable_logb, variable_logc, a, variable_fe, variable_y, variable_x)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RegressionResultsWrapper)
    
    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == 0.5
    assert round(result.params[0], 2) == 1.5

 ### Just one test as the three functions above are almost basically the same function
 
 


 

### Defining - distance dummies robust regression

# from model import reg_robust

def reg_robust(Data, variable_y, variable_x):
    
    """This is just a simple robust regression in which the Huber's T norm is one such robust estimator that gives less weight 
    to outliers and more weight to inliers when calculating the scale of the data. We have our Data set (Data), our exogenous
    variable (variable_x) and our endogenous variable(variable_y)."""
    
    y = Data[variable_y]
    x = Data[variable_x]
    robust_model = smm.RLM(y, x, M=smm.robust.norms.HuberT())
    reg = robust_model.fit()
    params = reg.params
    return reg

@pytest.fixture
def input_data_reg_robust():
    Data = pd.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [1, 3, 2, 5, 10]
    })
    variable_y = Data['y']
    variable_x = Data['x']
    return Data, variable_y, variable_x

def test_reg_robust(input_data_reg_robust):
    Data, variable_y, variable_x = input_data_reg_robust
    
    # Implementing the regression
    result = reg_robust(Data, variable_y, variable_x)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RLMResultsWrapper)
    
    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == 1.35
    
    




# Defining regression with clusters

# from model import areg_clus

def areg_clus(Data, variable_y, variable_x):
    
    """This is just a simple regression with clusters and no condition on the data set. We have the same cluster variable throughout the code
    therefore, it is already embedded in the function Data['observ']. We have our Data set (Data), our exogenous
    variable (variable_x) and our endogenous variable(variable_y). """
    
    y = Data[variable_y]
    x = Data[variable_x]
    X = smm.add_constant(x) # adding a constant to the Xs
    reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': Data['observ']}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
    #cov_kwds={'groups': fixed_effects} argument specifies the group variable to use in computing the cluster-robust standard errors
    params = reg.params
    return reg

@pytest.fixture
def input_data_areg_clus():
    Data = pd.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [1, 3, 2, 5, 10],
        
    })
    variable_y = Data['y']
    variable_x = Data['x']
    return Data, variable_y, variable_x

def test_areg_clus(input_data_areg_clus):
    
    Data, variable_y, variable_x = input_data_areg_clus = input_data_areg_clus

    # Implementing the regression
    result = areg_clus(Data, variable_y, variable_x)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RegressionResultsWrapper)
    
    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == 1.77
    
   
    


### Defining another function with clusters

# from model import areg_clus_abs

def areg_clus_abs(Data, drop_subset, y_variable, x_variable, dummy_variable):
    
    """This is just another simple regression with clusters and no condition on the data set.  In this case we approach the regression
    with the usage of dummy variables to be used for the clustering. In this case we select the dummy variable (dummy_variable) to be used.
    We also drop a subset of data that will not be needed for the regression (drop_subset). 
    We have our Data set (Data), our exogenous variable (variable_x) and our endogenous variable(variable_y). """
    
    df = Data.dropna(subset=drop_subset)
    y = df[y_variable]
    x = df[x_variable]
    dummies = pd.get_dummies(df[dummy_variable])
    X = pd.concat([x, dummies], axis=1)
    reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': df[dummy_variable]}, use_t=True) # with cluster for observ
    params = reg.params
    return reg

@pytest.fixture
def input_data_areg_clus_abs():
    # generate a sample dataset for testing
    Data = pd.DataFrame({'y': [1, 2, 3], 'x': [4, 5, 6], 'z': [7, 8 ,9], 'dummy': [1, 1, 2]})
    y_variable = Data['y']
    x_variable = Data['x']
    dummy_variable = Data['dummy']
    drop_subset = Data['z']
    return Data, drop_subset, y_variable, x_variable, dummy_variable

def test_areg_clus_abs(input_data_areg_clus_abs):
    Data, drop_subset, y_variable, x_variable, dummy_variable = input_data_areg_clus_abs
    
    # Implementing the regression
    result = areg_clus_abs(Data, drop_subset, y_variable, x_variable, dummy_variable)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RegressionResultsWrapper)
    
    
    


# Defining the poisson regression

# from model import poisson_reg

def poisson_reg(Data, y_variable, x_variable, index_variables, type_of_possion, weight, x_irra):
    
    """This performs a fixed effects poisson regression for different types of conditions reflected in (type_of_poisson_input); 
    whether it is a simple fixed effects poisson regression, or is it weighther by a variable, in whiich case we input (weight) or is 
    if we need to calculate interrater reliability (IRR) coefficients for a given set of data we add (x_irra)
    We have our Data set (Data), our exogenous variable (variable_x) and our endogenous variable(variable_y) and our 
    index variables (index_variables) which creates a pandas Multiindex for panel data """
    
    data = Data.set_index(index_variables) # create a pandas MultiIndex for panel data
    Y = data[y_variable]
    X = data[x_variable]
    if type_of_possion == "fixed effects":
        model = PanelOLS(Y, X, entity_effects=True, time_effects=True, drop_absorbed=True) # drop_absorbed=True is to drop any variables that could create multicollinearity (and therefore the matrix cannot be solved)
        reg = model.fit(cov_type='clustered', cluster_entity=True)
        params = reg.params
        return reg, params
    elif type_of_possion == "fixed effects weighted":        
        w = Data[weight] # weights added
        data['iweight'] = w 
        model = PanelOLS(Y, X, entity_effects=True, time_effects=True, drop_absorbed=True, weights=data['w']) # drop_absorbed=True is to drop any variables that could create multicollinearity (and therefore the matrix cannot be solved)
        reg = model.fit(cov_type='clustered', cluster_entity=True)
        params = reg.params
        return reg, params
    elif type_of_possion == "fixed effects weighted irr": 
        w = Data[weight] # weights added
        data['iweight'] = w 
        model = PanelOLS(Y, X, entity_effects=True, time_effects=True, drop_absorbed=True, weights=data['w']) # drop_absorbed=True is to drop any variables that could create multicollinearity (and therefore the matrix cannot be solved)
        reg = model.fit(cov_type='clustered', cluster_entity=True)
        params = reg.params
        predictions = reg.predict(X[x_irra]) # The months were deleted from our regression given that they cause multicollinearity, meaning, they do not add any new explanatory info
        irr_predictions = np.exp(predictions) # function will exponentiate the predicted values obtained from the predict() function. This will convert the results into incidence rate ratios.
        return reg, params, predictions, irr_predictions
    
@pytest.fixture
def input_data_poisson_reg():
    # generate a sample dataset for testing
    Data = pd.DataFrame({
        'index1': [1, 1, 2, 2, 3, 3],
        'index2': [2010, 2011, 2010, 2011, 2010, 2011],
        'y': [3, 4, 1, 2, 5, 6],
        'x': [2, 3, 4, 5, 6, 7],
    })
    return Data

def test_poisson_reg(input_data_poisson_reg):
    
    # results of poisson for fixed effects
    result_fe, params_fe = poisson_reg(input_data_poisson_reg, 'y', 'x', ['index1', 'index2'], 'fixed effects', None, None)
    # checking that they are pulled correctly and have the right data type
    assert isinstance(result_fe, PanelOLS)
    assert isinstance(params_fe, pd.Series)

    # results of poisson for fixed effects weighted
    result_fe_w, params_fe_w = poisson_reg(input_data_poisson_reg, 'y', 'x', ['index1', 'index2'], 'fixed effects weighted', 1, None) # 1 is for the weight
    # checking that they are pulled correctly and have the right data type
    assert isinstance(result_fe_w, PanelOLS)
    assert isinstance(params_fe_w, pd.Series)

    # results of poisson for fixed effects weighted irr
    result_fe_w_irr, params_fe_w_irr, pred, irr_pred = poisson_reg(input_data_poisson_reg, 'y', 'x', ['index1', 'index2'], 'fixed effects weighted irr', weight=1, x_irra='index2')
    # checking that they are pulled correctly and have the right data typ
    assert isinstance(result_fe_w_irr, PanelOLS)
    assert isinstance(params_fe_w_irr, pd.Series)
    assert isinstance(pred, pd.Series)
    assert isinstance(irr_pred, pd.Series)






