import numpy as np
import scipy as scy
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
    result = [t, p]
    return result



def regression_testing(regression, variable_test, testing_number):
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
        result = (
            f"The coefficient of {variable_test}] is significantly different from {testing_number} with p-value",
            pvalue,
        )
        resulting = {"variable_to_test": f"{variable_test}", "number to be tested": f"{testing_number}", "tvalue": tvalue, "p-value": pvalue}
    else:
        result = (
            f"The coefficient of {variable_test} is not significantly different from {testing_number} with p-value",
            pvalue,
        )
    results = [tvalue, pvalue, result]
    return results




def summarize_data(df):
    """This function summarizes the total number of thefts in a pandas DataFrame based
    on certain conditions.

    Args:
    - df (pandas.DataFrame): The DataFrame containing the data to summarize.

    Returns:
    - A pandas Series object containing descriptive statistics of the total number of thefts,
      including count, mean, standard deviation, minimum, and maximum values.

    """
    summary = df[
        (df["month"] > 7)
        & (df["jewish_inst"] == 0)
        & (df["jewish_inst_one_block_away_1"] == 0)
        & (df["distance_to_jewish_inst"] == 0)
        & ((df["totalpre"] != 0) | (df["totalpos"] != 0))
    ]["total_thefts"].describe()
    return summary



