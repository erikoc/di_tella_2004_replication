import numpy as np
import pandas as pd
import pyreadstat  as pyread
from pandas import DataFrame as df
from linearmodels.panel import PanelOLS
import scipy as scy
from scipy import stats
import statsmodels.api as smm
import statsmodels.formula.api as sm
from statsmodels.formula.api import ols
from linearmodels.panel import FirstDifferenceOLS


""" Function used Monthly Panel """

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

# Defining Regression Test Function

def testings(regression, variable_test, testing_number):
    
    """" What this function is doing is just to perform a t-statistic test with the coefficients of a regression (regression) in which it is checked 
    whether a variable (variable_test) is statistically close in value to a certain fixed value (testing_number) by us (in this case
    by the authors of the paper)"""
    
    tvalue = (regression.params[variable_test] - (testing_number)) / regression.bse[variable_test]
    pvalue = 2 * (1 - scy.stats.t.cdf(np.abs(tvalue), regression.df_resid))
    if pvalue < 0.05:
        print("The coefficient for institu1 is significantly different from -.08080 with p-value", pvalue)
    else:
        print("The coefficient for institu1 is not significantly different from -0.08080 with p-value", pvalue)
        
# Defing areg, FIXED EFFECTS AND CLUSTERS

def areg(Data, type_condition, variable_log, variable_loga, variable_logb, variable_logc, a, variable_fe, variable_y, variable_x):
    
    """" What this function is doing is performing a fixed effects regression with clustered covariance of these. 
    It requires a variable (variable_fe) that will be used as the fixed effects for the clustered variance. It uses a Data set (Data) in order to perform 
    the regression. Then, there is a condition that should be met ("type_condition) in order for the function to asses
    which which condition/restrictions on the data used for the regression. Depending on the condition, we have different input
    variables (variable_log, variable_loga, variable_logb, variable_log). We also have "a" which is the value of the varibles that should
    be met in order to constraint the data set. Finally we have our exogenous variable (variable_x) and our endogenous avriable (variable_y) for the regression."""
    
    if type_condition == "single":
        condition = Data[variable_log] == a
        m = Data.loc[condition] # the logical condition that must be accomplished to do the regression in the data
        fixed_effects = Data[variable_fe][m] # observ is used for our fixed effects only for those lines where institu1==1
        y = MonthlyPanel[variable_y]
        x = MonthlyPanel[variable_x]
        X = smm.add_constant(x) # adding a constant to the Xs
        reg = smm.OLS(y[m], X[m]).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
    #cov_kwds={'groups': fixed_effects} argument specifies the group variable to use in computing the cluster-robust standard errors
        params = reg.params
        return params
    elif type_condition == "double or":
        condition1 = Data[variable_loga] == a
        condition2 = Data[variable_logb] == a
        m = Data.loc[(condition1) | (condition2)] # the logical condition that must be accomplished to do the regression in the data
        fixed_effects = Data[variable_fe][m] # observ is used for our fixed effects only for those lines where institu1==1
        y = MonthlyPanel[variable_y]
        x = MonthlyPanel[variable_x]
        X = smm.add_constant(x) # adding a constant to the Xs
        reg = smm.OLS(y[m], X[m]).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
    #cov_kwds={'groups': fixed_effects} argument specifies the group variable to use in computing the cluster-robust standard errors
        params = reg.params
        return params
    elif type_condition == "double or different":
        condition1 = Data[variable_loga] != a
        condition2 = Data[variable_logb] != a
        m = Data.loc[(condition1) | (condition2)] # the logical condition that must be accomplished to do the regression in the data
        fixed_effects = Data[variable_fe][m] # observ is used for our fixed effects only for those lines where institu1==1
        y = MonthlyPanel[variable_y]
        x = MonthlyPanel[variable_x]
        X = smm.add_constant(x) # adding a constant to the Xs
        reg = smm.OLS(y[m], X[m]).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
    #cov_kwds={'groups': fixed_effects} argument specifies the group variable to use in computing the cluster-robust standard errors
        params = reg.params
        return params
    elif type_condition == "triple or":
        condition1 = Data[variable_loga] == a
        condition2 = Data[variable_logb] == a
        condition3 = Data[variable_logc] == a
        m = Data.loc[(condition1) | (condition2) | (condition3)] # the logical condition that must be accomplished to do the regression in the data
        fixed_effects = Data[variable_fe][m] # observ is used for our fixed effects only for those lines where institu1==1
        y = MonthlyPanel[variable_y]
        x = MonthlyPanel[variable_x]
        X = smm.add_constant(x) # adding a constant to the Xs
        reg = smm.OLS(y[m], X[m]).fit(cov_type='cluster', cov_kwds={'groups': fixed_effects}) # sm.OLS is used to run a simple linear regression, cluster is used to compute cluster-robust standard errors, 
    #cov_kwds={'groups': fixed_effects} argument specifies the group variable to use in computing the cluster-robust standard errors
        params = reg.params
        return params 

### Defining - distance dummies robust regression

def reg_robust(Data, variable_y, variable_x):
    
    """This is just a simple robust regression in which the Huber's T norm is one such robust estimator that gives less weight 
    to outliers and more weight to inliers when calculating the scale of the data. We have our Data set (Data), our exogenous
    variable (variable_x) and our endogenous variable(variable_y)."""
    
    y = Data[variable_y]
    x = Data[variable_x]
    robust_model = smm.RLM(y, x, M=smm.robust.norms.HuberT())
    robust_results = robust_model.fit()
    params = robust_results.params
    return params

# Defining regression with clusters

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
    return params

### Defining another function with clusters
def areg_clus_abs(Data, drop_subset, y_variable, x_variable, dummy_variable):
    
    """This is just another simple regression with clusters and no condition on the data set.  In this case we approach the regression
    with the usage of dummy variables to be used for the clustering. In this case we select the dummy variable (dummy_variable) to be used.
    We also drop a subset of data that will not be needed for the regression (drop_subset). 
    We have our Data set (Data), our exogenous variable (variable_x) and our endogenous variable(variable_y). """
    
    df = Data.dropna(subset=drop_subset)
    df = df.astype(float)
    y = df[y_variable]
    x = df[x_variable]
    dummies = pd.get_dummies(df[dummy_variable])
    X = pd.concat([x, dummies], axis=1)
    reg = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': df[dummy_variable]}, use_t=True) # with cluster for observ
    params = reg.params
    return params

# Defining the poisson regression

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
        results = model.fit(cov_type='clustered', cluster_entity=True)
        params = results.params
        return params
    elif type_of_possion == "fixed effects weighted":        
        w = Data[weight] # weights added
        data['iweight'] = w 
        model = PanelOLS(Y, X, entity_effects=True, time_effects=True, drop_absorbed=True, weights=data['w']) # drop_absorbed=True is to drop any variables that could create multicollinearity (and therefore the matrix cannot be solved)
        results = model.fit(cov_type='clustered', cluster_entity=True)
        params = results.params
        return params
    elif type_of_possion == "fixed effects weighted irr": 
        w = Data[weight] # weights added
        data['iweight'] = w 
        model = PanelOLS(Y, X, entity_effects=True, time_effects=True, drop_absorbed=True, weights=data['w']) # drop_absorbed=True is to drop any variables that could create multicollinearity (and therefore the matrix cannot be solved)
        results = model.fit(cov_type='clustered', cluster_entity=True)
        predictions = results.predict(X[x_irra]) # The months were deleted from our regression given that they cause multicollinearity, meaning, they do not add any new explanatory info
        irr_predictions = np.exp(predictions) # function will exponentiate the predicted values obtained from the predict() function. This will convert the results into incidence rate ratios.
        return irr_predictions


""" Monthly Panel """ 

############################################## PART 1 ########################################################################################################################

# Calling the required dataframe
MonthlyPanel = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel.csv')

# table otromes1 if mes~=72, by(codigo) c(mean totrob2 sd totrob2);
MP = MonthlyPanel.apply(pd.to_numeric, errors='coerce') # replacing non numeric values of totrob2 with NAs
MP.loc[MP['month']!=72].groupby(['othermonth1', 'code'])['total_thefts2'].agg(['mean', 'std'])

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
MonthlyPanel2 = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel2.csv')

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
type_condition1 = "single"
variable_log1 = ["jewish_inst"]
a1 = 1
variable_fe1 = ["observ"]
variable_y1 = ["total_thefts_c"]
variable_x1 = ["jewish_inst_p"]
regression_fe1 = areg(Data=Data1, type_condition=type_condition1, variable_log=variable_log1, a=a1, variable_fe=variable_fe1, variable_y=variable_y1, variable_x=variable_x1)

# areg totrobc inst1p inst3_1p if (institu1==1 | inst3_1==1), absorb(observ) robust;
Data2 = MonthlyPanel2
type_condition2 = "double or"
variable_log_1 = ["jewish_inst"]
variable_log_2 = ["jewish_int_one_block_away_1"]
a2 = 1
variable_fe2 = ["observ"]
variable_y2 = ["total_thefts_c"]
variable_x2 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
regression_fe2 = areg(Data=Data2, type_condition=type_condition2, variable_loga=variable_log_1, variable_logb=variable_log_2, a=a2, variable_fe=variable_fe2, variable_y=variable_y2, variable_x=variable_x2)

# areg totrobc inst1p inst3_1p cuad2p if (institu1==1 | inst3_1==1 | cuad2==1), absorb(observ) robust;
Data3 = MonthlyPanel2
type_condition3 = "triple or"
variable_log_3 = ["jewish_inst"]
variable_log_4 = ["jewish_int_one_block_away_1"]
variable_log_5 = ["cuad2"]
a3 = 1
variable_fe3 = ["observ"]
variable_y3 = ["total_thefts_c"]
variable_x3 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p"]
regression_fe3 = areg(Data=Data3, type_condition=type_condition3, variable_loga=variable_log_3, variable_logb=variable_log_4, variable_logc=variable_log_5, a=a3, variable_fe=variable_fe3, variable_y=variable_y3, variable_x=variable_x3)

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
x_clu1 = ["jewish_inst_p", 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
regression_clu1 = areg_clus(Data_clu1, y_clu1, x_clu1)

# areg totrob inst1p inst3_1p month*, absorb(observ) robust;
Data_clu2 = MonthlyPanel2
y_clu2 = "total_thefts"
x_clu2 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
regression_clu2 = areg_clus(Data_clu2, y_clu2, x_clu2)

# areg totrob inst1p inst3_1p cuad2p month*, absorb(observ) robust;
Data_clu3 = MonthlyPanel2
y_clu3 = "total_thefts"
x_clu3 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p", 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
regression_clu3 = areg_clus(Data_clu3, y_clu3, x_clu3)

### TESTS

# test inst1p=-0.01221;
variable_test7 = "jewish_inst_p"
testing_number7 = -0.01221
test_diff7 = testings(regression_clu1, variable_test7, testing_number7)

# test (inst1p/(161/37))=-inst3_1p;
variable_test8 = ("jewish_inst_p")/(161/37)
testing_number8 = -("jewish_inst_one_block_away_1_p")
test_diff8 = testings(regression_clu2, variable_test8, testing_number8)

# test (inst1p/(226/37))=-cuad2p;
variable_test9 = ("jewish_inst_p")/(226/37)
testing_number9 = -("cuad2p")
test_diff9 = testings(regression_clu3, variable_test9, testing_number9)

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
variable_y_robust1 = ["total_thefts"]
variable_x_robust1 = ['jewish_inst', 'jewish_int_one_block_away_1', 'cuad2', 'jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
reg_robust1 = reg_robust(MonthlyPanel)

### MORE CLUSTERED REGRESSIONS

### areg we can use our previously defined function areg_clus()

# areg robcoll inst1p inst3_1p cuad2p month*, absorb(observ) robust;
Data_clu4 = MonthlyPanel2
y_clu4 = "theftscoll"
x_clu4 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p", "cuad2p", 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
regression_clu4 = areg_clus(Data_clu4, y_clu4, x_clu4)

# areg totrob inst1p inst3_1p cuad2p month*, absorb(observ) robust cluster(observ);

Data_clus_abs1 = MonthlyPanel2
drop_subset_clus_abs1 = ['total_thefts', 'jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
y_variable_clus_abs1 = ["total_thefts"]
x_variable_clus_abs1 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
dummy_variable_clus_abs1 = ['observ']
regression_clus_abs1 = areg_clus_abs(Data_clus_abs1, drop_subset_clus_abs1, y_variable_clus_abs1, x_variable_clus_abs1, dummy_variable_clus_abs1)

# summarize totrob if mes>7 & (institu1==0 & inst3_1==0 & cuad2==0 & (totpre~=0 | totpos~=0));
summary =  MonthlyPanel2[(MonthlyPanel2['month'] > 7) & (MonthlyPanel2['jewish_inst'] == 0) & (MonthlyPanel2['jewish_inst_one_block_away_1'] == 0) & (MonthlyPanel2['cuad2'] == 0) & ((MonthlyPanel2['totalpre'] != 0) | (MonthlyPanel2['totalpos'] != 0))]['total_thefts'].describe()
summary # summary of totrob given the coditions above specified

# areg totrob inst1p inst3_1p cuad2p month* if (totpre~=0 | totpos~=0), absorb(observ) robust; # We can use fixed effects
Data4 = MonthlyPanel2
type_condition4 = "double or different"
variable_log_6 = ["totalpre"]
variable_log_7 = ["totalpos"]
a4 = 0
variable_fe4 = ["observ"]
variable_y4 = ["total_thefts"]
variable_x4 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
regression_fe4 = areg(Data=Data4, type_condition=type_condition4, variable_loga=variable_log_6, variable_logb=variable_log_7, a=a4, variable_fe=variable_fe4, variable_y=variable_y4, variable_x=variable_x4)
    
# xtpois totrob inst1p inst3_1p cuad2p month*, fe i(observ);
Data_poisson1 = MonthlyPanel2
y_variable_poisson1 = "total_thefts"
x_variable_poission1 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
index_variables_poisson1 = ['observ', 'month']
type_of_possion1 = "fixed effects"
reg_poisson1 = poisson_reg(Data=Data_poisson1, y_variable=y_variable_poisson1, x_variable=x_variable_poission1, index_variables=index_variables_poisson1, type_of_possion=type_of_possion1)

# # xtpois totrobq inst1p inst3_1p cuad2p month* [iweight=w], fe i(observ);
Data_poisson2 = MonthlyPanel2
y_variable_poisson2 = "total_thefts"
x_variable_poission2 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
index_variables_poisson2 = ['observ', 'month']
type_of_possion2 = "fixed effects weighted"
weight_poisson = 'w'
reg_poisson2 = poisson_reg(Data=Data_poisson2, y_variable=y_variable_poisson2, x_variable=x_variable_poission2, index_variables=index_variables_poisson2, type_of_possion=type_of_possion2, weight=weight_poisson)

# xtpois totrobq inst1p inst3_1p cuad2p month* [iweight=w], fe i(observ) irr;
Data_poisson3 = MonthlyPanel2
y_variable_poisson3 = "total_thefts"
x_variable_poission3 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
index_variables_poisson3 = ['observ', 'month']
type_of_possion3 = "fixed effects weighted irr"
weight_poisson2 = 'w'
x_irra_poisson = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p']
reg_poisson3 = poisson_reg(Data=Data_poisson3, y_variable=y_variable_poisson3, x_variable=x_variable_poission3, index_variables=index_variables_poisson3, type_of_possion=type_of_possion3, weight=weight_poisson2, x_irra=x_irra_poisson)

# areg totrob inst1p inst3_1p cuad2p month*, absorb(observ) robust cluster(codigo2); # We can use the function areg_clus_abs
Data_clus_abs2 = MonthlyPanel2
drop_subset_clus_abs2 = ['total_thefts', 'jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
y_variable_clus_abs2 = ["total_thefts"]
x_variable_clus_abs2 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12']
dummy_variable_clus_abs2 = ['code2']
regression_clus_abs2 = areg_clus_abs(Data_clus_abs2, drop_subset_clus_abs2, y_variable_clus_abs2, x_variable_clus_abs2, dummy_variable_clus_abs2)

# areg totrob inst1p inst3_1p cuad2p mbelg* monce* mvcre*, absorb(observ) robust;
from clean_data import list_names_place
Data_clu5 = MonthlyPanel2
y_clu5 = "total_thefts"
x_clu5 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p']
x_clu5.extend(list_names_place)
regression_clu5 = areg_clus(Data_clu5, y_clu5, x_clu5)

############################################## PART 3 ########################################################################################################################

# Calling the required dataframe
MonthlyPanel3 = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel3.csv')

# areg totrob nepin1p nepi3_1p nepcua2p epin1p epin3_1p epcuad2p month*, absorb(observ) robust; # USE areg_clus function
# areg totrob nesin1p nesi3_1p nescua2p esin1p esin3_1p escuad2p month*, absorb(observ) robust;
# areg totrob nbain1p nbai3_1p nbacua2p bain1p bain3_1p bacuad2p month*, absorb(observ) robust;
# areg totrob ntoin1p ntoi3_1p ntocua2p toin1p toin3_1p tocuad2p month*, absorb(observ) robust;

# Calling the necessary list
from clean_data import list_names_data3_1 
from clean_data import list_names_data3_2 
from clean_data import list_names_data3_3 
from clean_data import list_names_data3_4 

# Regression
reg_results = {} # To save the regression results

for i in range(6,10):
    Data_used = MonthlyPanel3
    y_clu = "total_thefts"
    x_clu = [f"list_names_data3_{i}"]
    reg_results[f"regression_clu{i}"] = areg_clus(Data_used, y_clu, x_clu)

# Tests
test_results = {} # To save the tests results

for i , j in zip(range(6,10), range(1,5)):
    for x, y, a in zip([0,1,2], [3,4,5], ["p", "1_p", "cuad2p"]):
        variable_test = (f"list_names_data3_{j}"[y])
        testing_number = (f"list_names_data3_{j}"[x])
        test_results[f"test_diff{i}_{a}"] = testings(reg_results[f"regression_clu{i}"], variable_test, testing_number)
        
############################################## PART 4 ########################################################################################################################

# Calling the required dataframe
MonthlyPanel_new = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel_new.csv')
       
# summarize totrob;
MonthlyPanel_new['total_thefts'].describe()

# areg totrob 1inst1p 1inst3_1p 1cuad2p month*, absorb(observ) robust; # USE areg_clus function
# areg totrob 2inst1p 2inst3_1p 2cuad2p month*, absorb(observ) robust;
# areg totrob 3inst1p 3inst3_1p 3cuad2p month*, absorb(observ) robust;

Data_clu_new = MonthlyPanel_new
y_clu_new = "total_thefts"
x_clu_new1 = ['one_jewish_inst_1_p', 'one_jewish_inst_one_block_away_1_p', 'one_cuad2p', 'month5', 'month6', 'month7']
x_clu_new2 = ['two_jewish_inst_1_p', 'two_jewish_inst_one_block_away_1_p', 'two_cuad2p', 'month5', 'month6', 'month7']
x_clu_new3 = ['three_jewish_inst_1_p', 'three_jewish_inst_one_block_away_1_p', 'three_cuad2p', 'month5', 'month6', 'month7']

regression_new1 = areg_clus(Data_clu_new, y_clu_new, x_clu_new1)   
regression_new2 = areg_clus(Data_clu_new, y_clu_new, x_clu_new2)  
regression_new3 = areg_clus(Data_clu_new, y_clu_new, x_clu_new3)  











""" Function used Weekly Panel """

# Generate a function that will get us the regression results


def regression_WeeklyPanel(Data, y_variable, type_of_regression):
    """This is just a simple fixed effects regression which performs two different actions depending on  whether we want a clustered or an unclustered regression.
    This is input via (type_of_regression) input. As fixed effects we know that 'observ' is normally used. Therefore, it is embedded in the function.
    We have our Data set (Data), our exogenous variable (variable_x) and our endogenous variable(variable_y)."""
    
    # Creating a list (using list comprehension) of the columns to be created
    list_names = ["week1"]
    #list_names.extend([list_names] + [f"month{i}" for i in range(6,13)])
    list_names.extend([f"week{i}" for i in range(2,40)])
    
    WeeklyP = Data[(Data['week']!=16) & (Data['week']!=17)]
    y = WeeklyP[y_variable]
    x = WeeklyP[['jewish_inst_p', 'jewish_int_one_block_away_1_p', 'cuad2p']] # inst1p = jewish_inst_p, inst3_1p = jewish_int_one_block_away_1_p
    x = list(x.columns) + list_names 
    x_1 = WeeklyP[x]
    if type_of_regression == "unclustered":
        # Check if the modified MonthlyPanel_new is a pandas DataFrame
        X = smm.add_constant(x_1)
        reg = smm.OLS(y, X)
        result = reg.fit(cov_type='cluster', cov_kwds={'groups': WeeklyP['observ']}, hasconst=True)
        params = result.params
        return params
    elif type_of_regression == "clustered":    
        dummies = pd.get_dummies(WeeklyP['code2']) # to capture the fixed efefcts by codigo2
        X = pd.concat([x_1, dummies], axis=1)
        result = smm.OLS(y, X).fit(cov_type='cluster', cov_kwds={'groups': WeeklyP['observ']}, use_t=True) # with cluster for observ
        params = result.params
        return params

""" Weekly Panel """

# Calling the required dataframe
WeeklyPanel = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Checking_my_code/Clean_data/WeeklyPanel.csv' )

# summarize total_thefts;
WeeklyPanel['total_thefts'].describe()

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








""" Crime by block """