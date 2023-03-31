import statsmodels.api as sm
from linearmodels.iv import absorbing


def abs_regression_models_weekly(df, type_of_regression):
    """Perform panel regression analysis for total thefts on the input dataframe using
    absorbing regression models, considering weekly dummy variables and different types
    of regression.

    Args:
     df (pandas.DataFrame): Input dataframe containing the necessary variables,
                            including 'tot_theft', 'treatment', 'treatment_1d',
                            'treatment_2d', 'block', and weekly dummy variables.
     type_of_regression (str): Type of regression to perform, either "robust" or "clustered".

    Returns:
    Fitted absorbing regression model results.

    """
    abs_model = absorbing.AbsorbingLS(
        df["total_thefts"],
        sm.add_constant(
            df[
                ["treatment", "treatment_1d", "treatment_2d"]
                + [f"week_dummy_{i}" for i in range(2, 39) if i not in [16, 17]]
            ],
        ),
        absorb=df["block"].to_frame().astype(float),
        drop_absorbed=True,
    )

    if type_of_regression == "robust":
        abs_results = abs_model.fit(cov_type="robust", debiased=True)
    elif type_of_regression == "clustered":
        abs_results = abs_model.fit(cov_type="clustered")

    return abs_results


def abs_regression_models_av_weekly(df):
    """Perform panel regression analysis for average weekly thefts on the input
    dataframe using absorbing regression models, considering weekly dummy variables and
    clustering by entity.

    Args:
    df (pandas.DataFrame): Input dataframe containing the necessary variables,
                           including 'av_weekly_thefts', 'treatment', 'treatment_1d',
                           'treatment_2d', 'block', and weekly dummy variables.

    Returns:
    Fitted absorbing regression model results with clustered standard errors.

    Example:
    Given a dataframe 'data' with the necessary variables, perform a panel regression analysis:

    """
    abs_model = absorbing.AbsorbingLS(
        df["av_weekly_thefts"],
        sm.add_constant(
            df[
                ["treatment", "treatment_1d", "treatment_2d"]
                + [f"week_dummy_{i}" for i in range(2, 39) if i not in [16, 17]]
            ],
        ),
        absorb=df["block"].to_frame().astype(float),
        drop_absorbed=True,
    )

    return abs_model.fit(cov_type="clustered")
