##############################################################################################################################################################################
"""Functions for fitting the regression model."""

import statsmodels.formula.api as smf
from statsmodels.iolib.smpickle import load_pickle
##############################################################################################################################################################################



import pandas as pd
from pandas import DataFrame as df
from linearmodels.panel import PanelOLS
import statsmodels.api as smm

"""Functions for fitting the regression model."""

from clean_data import WeeklyPanel
from clean_data import list_names





##############################################################################################################################################################################
def fit_logit_model(data, data_info, model_type):
    """Fit a logit model to data.

    Args:
        data (pandas.DataFrame): The data set.
        data_info (dict): Information on data set stored in data_info.yaml. The
            following keys can be accessed:
            - 'outcome': Name of dependent variable column in data
            - 'outcome_numerical': Name to be given to the numerical version of outcome
            - 'columns_to_drop': Names of columns that are dropped in data cleaning step
            - 'categorical_columns': Names of columns that are converted to categorical
            - 'column_rename_mapping': Old and new names of columns to be renamend,
                stored in a dictionary with design: {'old_name': 'new_name'}
            - 'url': URL to data set
        model_type (str): What model to build for the linear relationship of the logit
            model. Currently implemented:
            - 'linear': Numerical covariates enter the regression linearly, and
            categorical covariates are expanded to dummy variables.

    Returns:
        statsmodels.base.model.Results: The fitted model.

    """
    outcome_name = data_info["outcome"]
    outcome_name_numerical = data_info["outcome_numerical"]
    feature_names = list(set(data.columns) - {outcome_name, outcome_name_numerical})

    if model_type == "linear":
        # smf.logit expects the binary outcome to be numerical
        formula = f"{outcome_name_numerical} ~ " + " + ".join(feature_names)
    else:
        message = "Only 'linear' model_type is supported right now."
        raise ValueError(message)

    return smf.logit(formula, data=data).fit()


def load_model(path):
    """Load statsmodels model.

    Args:
        path (str or pathlib.Path): Path to model file.

    Returns:
        statsmodels.base.model.Results: The stored model.

    """
    return load_pickle(path)
##############################################################################################################################################################################









""" Monthly Panel """ 

""" Weekly Panel """

# summarize total_thefts;
WeeklyPanel['total_thefts'].describe()

# Generate a function that will get us the regression results

def regression_WeeklyPanel(Data,type_of_data, type_of_regression):
    WeeklyP = Data[(Data['week']!=16) & (Data['week']!=17)]
    if type_of_data == "total_thefts":
        y = WeeklyP['total_thefts']
        x = WeeklyP[['jewish_inst_p', 'jewish_int_one_block_away_1_p', 'cuad2p']] # inst1p = jewish_inst_p, inst3_1p = jewish_int_one_block_away_1_p
        x = list(x.columns) + list_names 
        x_1 = WeeklyP[x]
    elif type_of_data == "n_total_thefts":
        y = WeeklyP['n_total_thefts']
        x = WeeklyP[['jewish_inst_p', 'jewish_int_one_block_away_1_p', 'cuad2p']] # inst1p = jewish_inst_p, inst3_1p = jewish_int_one_block_away_1_p
        x = list(x.columns) + list_names 
        x_1 = WeeklyP[x]
    if type_of_regression == "unclustered":
        X = smm.add_constant(x_1)
        reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': WeeklyP['observ']}, hasconst=True)
        params = reg.params
        return params
    elif type_of_regression == "clustered":    
        dummies = smm.get_dummies(WeeklyP['codigo2']) # to capture the fixed efefcts by codigo2
        X = pd.concat([x_1, dummies], axis=1)
        reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': WeeklyP['observ']}, use_t=True) # with cluster for observ
        params = reg.params
        return params  

# areg totrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust;
Data = WeeklyPanel
type_of_data = "total_thefts"
type_of_regression = "unclustered"
regression_1 = regression_WeeklyPanel(Data,type_of_data, type_of_regression)

# areg totrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust cluster(codigo2);
Data = WeeklyPanel
type_of_data = "total_thefts"
type_of_regression = "clustered"
regression_2 = regression_WeeklyPanel(Data,type_of_data, type_of_regression)

# areg ntotrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust cluster(codigo2);
Data = WeeklyPanel
type_of_data = "n_total_thefts"
type_of_regression = "clustered"
regression_3 = regression_WeeklyPanel(Data,type_of_data, type_of_regression)

""" Crime by block """