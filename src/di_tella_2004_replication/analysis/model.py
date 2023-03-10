import numpy as np
import pandas as pd
import pyreadstat  as pyread
from pandas import DataFrame as df
import scipy as scy
from scipy import stats
import statsmodels.api as smm
import statsmodels.formula.api as sm
from statsmodels.formula.api import ols
from linearmodels.panel import FirstDifferenceOLS

"""Functions for fitting the regression model."""




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

############################################## PART 1 ########################################################################################################################

# Calling the required dataframe
MonthlyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel.csv')

# table otromes1 if mes~=72, by(codigo) c(mean totrob2 sd totrob2);
MP = MonthlyPanel.apply(pd.to_numeric, errors='coerce') # replacing non numeric values of totrob2 with NAs
MP.loc[MP['month']!=72].groupby(['othermonth1', 'code'])['total_thefts2'].agg(['mean', 'std'])

# Defining WelchTest function
def WelchTest(Data, code1, code2):
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
MonthlyPanel2, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel2.csv')

# sum totrob if post==1 & distanci>2;
MonthlyPanel2.loc[(MonthlyPanel2['post'] == 1) & (MonthlyPanel2['distance_to_jewish_inst'] > 2), 'total_thefts'].sum()

# Regressions

# reg totrob institu1 month* if post==1, robust;
formula1="total_thefts ~ jewish_inst"
formula1 = '+'.join([formula1] + [f"month{i}" for i in range(5,13)]) # This is using a list comprehension
regression1 = sm.ols(formula1, data=MonthlyPanel2[MonthlyPanel2['post'] == 1]).fit()
# reg totrob institu1 inst3_1 month* if post==1, robust;
formula2="totrob ~ jewish_inst_one_block_away_1"
formula2 = '+'.join([formula2] + [f"month{i}" for i in range(5,13)]) # This is using a list comprehension
regression2 = sm.ols(formula2, data=MonthlyPanel2[MonthlyPanel2['post'] == 1]).fit()
# reg totrob institu1 inst3_1 cuad2 month* if post==1, robust;
formula3="totrob ~ jewish_inst + jewish_inst_one_block_away_1 + cuad2"
formula3 = '+'.join([formula3] + [f"month{i}" for i in range(5,13)]) # This is using a list comprehension
regression3 = sm.ols(formula3, data=MonthlyPanel2[MonthlyPanel2['post'] == 1]).fit()

""" Weekly Panel """

# Calling the required dataframe
WeeklyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/WeeklyPanel.csv')

# summarize total_thefts;
WeeklyPanel['total_thefts'].describe()

# Calling necessary variables from other files
from clean_data import list_names

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
type_of_data1 = "total_thefts"
type_of_regression1 = "unclustered"
regression_1 = regression_WeeklyPanel(Data,type_of_data1, type_of_regression1)

# areg totrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust cluster(codigo2);
Data = WeeklyPanel
type_of_data2 = "total_thefts"
type_of_regression2 = "clustered"
regression_2 = regression_WeeklyPanel(Data,type_of_data2, type_of_regression2)

# areg ntotrob inst1p inst3_1p cuad2p semana* if (week~=16 & week~=17), absorb(observ) robust cluster(codigo2);
Data = WeeklyPanel
type_of_data3 = "n_total_thefts"
type_of_regression3 = "clustered"
regression_3 = regression_WeeklyPanel(Data,type_of_data3, type_of_regression3)





""" Crime by block """