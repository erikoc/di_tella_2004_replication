import pandas as pd
import statsmodels.api as smm


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
