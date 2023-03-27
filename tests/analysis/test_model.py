import numpy as np
import pandas as pd
import pyreadstat  as pyread
import pytest
import scipy as scy
import statsmodels.api as smm
import statsmodels.formula.api as sm
from linearmodels.panel import PanelOLS
from scipy import stats
import io
import sys


from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.analysis.model import (
    regression_WeeklyPanel, # for WeeklPanel  
    WelchTest, testings, areg_single, areg_triple, reg_robust, areg_clus, areg_clus_abs, poisson_reg
)


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

"Fixtures"

@pytest.fixture
def input_data_WelchTest():   
    Data = pd.DataFrame({'code': [1, 1, 2, 2],
                         'month': [1, 2, 1, 2],
                         'total_thefts': [10, 12, 8, 9]})
    code1 = 1
    code2 = 2
    return Data, code1, code2
    
    
@pytest.fixture
def input_data_testings():
    data = {'var_y': [1, 2, 3, 4, 5],
            'var_x': [10, 20, 30, 40, 50],
            'fixed_effects': [1, 1, 2, 2, 3],
            'variable_log': [1, 1, 1, 0, 0]}
    df = pd.DataFrame(data)
    return df
    
    
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
    
    
@pytest.fixture
def input_data_reg_robust():
    Data = pd.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [1, 3, 2, 5, 10]
    })
    variable_y = Data['y']
    variable_x = Data['x']
    return Data, variable_y, variable_x


@pytest.fixture
def input_data_areg_clus():
    Data = pd.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [1, 3, 2, 5, 10],
        
    })
    variable_y = Data['y']
    variable_x = Data['x']
    return Data, variable_y, variable_x


@pytest.fixture
def input_data_areg_clus_abs():
    # generate a sample dataset for testing
    Data = pd.DataFrame({'y': [1, 2, 3], 'x': [4, 5, 6], 'z': [7, 8 ,9], 'dummy': [1, 1, 2]})
    y_variable = Data['y']
    x_variable = Data['x']
    dummy_variable = Data['dummy']
    drop_subset = Data['z']
    return Data, drop_subset, y_variable, x_variable, dummy_variable


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




"Tests"

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



def test_testings_areg_single(input_data_testings): # We use the testing applied to the areg_single regression below tested
    result = areg_single(Data=input_data_testings, variable_log='variable_log', a=1, variable_fe='fixed_effects', variable_y='var_y', variable_x='var_x') # example using the fixed effects regression
    assert result.params[1] == 1.0 # precalculated value for x coefficient
    assert result.params[0] == -1.0 # pre calculated value for the intercept
    
    # temporarily redirect stdout to a StringIO object
    output = io.StringIO()
    sys.stdout = output

    # call the testings function and pass the regression result, variable name, and testing value
    testings(result, 'var_x', 0)

    # get the printed output from the StringIO object
    printed_output = output.getvalue().strip()

    # assert that the printed output is correct
    assert printed_output == "The coefficient of var_x is significantly different from 0 with p-value 0.0"

    # reset stdout to its original value
    sys.stdout = sys.__stdout__


def test_areg_triple(input_data_areg_triple): 
    Data, variable_loga, variable_logb, variable_logc, a, variable_fe, variable_y, variable_x = input_data_areg_triple
    
    # Implementing the regression
    result = areg_triple(Data, variable_loga, variable_logb, variable_logc, a, variable_fe, variable_y, variable_x)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RegressionResultsWrapper)
    
    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == 0.5
    assert round(result.params[0], 2) == 1.5


def test_reg_robust(input_data_reg_robust):
    Data, variable_y, variable_x = input_data_reg_robust
    
    # Implementing the regression
    result = reg_robust(Data, variable_y, variable_x)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RLMResultsWrapper)
    
    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == 1.35
    

def test_areg_clus(input_data_areg_clus):
    
    Data, variable_y, variable_x = input_data_areg_clus = input_data_areg_clus

    # Implementing the regression
    result = areg_clus(Data, variable_y, variable_x)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RegressionResultsWrapper)
    
    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == 1.77
    

def test_areg_clus_abs(input_data_areg_clus_abs):
    Data, drop_subset, y_variable, x_variable, dummy_variable = input_data_areg_clus_abs
    
    # Implementing the regression
    result = areg_clus_abs(Data, drop_subset, y_variable, x_variable, dummy_variable)
    
    # Assert that it is performing the type of regression that we wanted
    assert isinstance(result, smm.regression.linear_model.RegressionResultsWrapper)


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






