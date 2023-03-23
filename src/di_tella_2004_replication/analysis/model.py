"""Functions for fitting the regression model."""
import statsmodels.api as sm
from linearmodels import PanelOLS
from linearmodels.iv import absorbing


def fe_regression_models(df):
    """Perform panel regression analysis on the input dataframe using different
    regression models for each suffix in the list.

    Args:
    df (pandas.DataFrame): Input dataframe with the necessary variables

    Returns:
    A tuple containing two dictionaries with the results of the fixed effects models and the absorbing fixed effects models, respectively.

    """
    fe_results = {}

    for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:

        fe_model = PanelOLS(
            df[f"tot_theft_{suffix}"],
            sm.add_constant(
                df[
                    ["treatment", "treatment_1d", "treatment_2d"]
                    + [f"month_dummy_{i}" for i in range(5, 13)]
                ],
            ),
            entity_effects=True,
            check_rank=True,
        )
        fe_results[suffix] = fe_model.fit(cov_type="clustered", cluster_entity=True)

    return fe_results


def abs_regression_models(df):
    """Perform panel regression analysis on the input dataframe using different
    regression models for each suffix in the list.

    Args:
    df (pandas.DataFrame): Input dataframe with the necessary variables

    Returns:
    A tuple containing two dictionaries with the results of the fixed effects models and the absorbing fixed effects models, respectively.

    """
    abs_results = {}

    for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:

        abs_model = absorbing.AbsorbingLS(
            df[f"tot_theft_{suffix}"],
            sm.add_constant(
                df[
                    ["treatment", "treatment_1d", "treatment_2d"]
                    + [f"month_dummy_{i}" for i in range(5, 13)]
                ],
            ),
            absorb=df["block"].to_frame().astype(int),
            drop_absorbed=True,
        )

        abs_results[suffix] = abs_model.fit(cov_type="robust", debiased=True)

    return abs_results
