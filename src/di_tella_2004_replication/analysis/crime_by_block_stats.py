from scipy import stats


def t_tests_crime_by_block(df):
    """Calculates two-sample t-tests for the given variables in the DataFrame `df`,
    between the groups where `jewish_inst == 1` and `jewish_inst == 0`.

    Args:
        df (pandas.DataFrame): A DataFrame containing the variables of interest.

    Returns:
        dict: A dictionary containing the results of the t-tests. The keys are the
        variable names, and the values are the t-statistic and p-value for the
        t-test between the two groups.

    """
    group1_data = df.loc[df["jewish_inst"] == 1]
    group2_data = df.loc[df["jewish_inst"] == 0]
    results = {}

    for var in [
        "av_age",
        "female_rate",
        "ownership_rate",
        "overcrowd_rate",
        "unmet_basic_needs_rate",
        "av_hh_head_schooling",
        "unemployment_rate",
        "av_hh_size",
    ]:
        group1_var_data = group1_data[var]
        group2_var_data = group2_data[var]
        t_statistic, p_value = stats.ttest_ind(
            group1_var_data,
            group2_var_data,
            equal_var=False,
        )

        results[var] = {"t_statistic": t_statistic, "p_value": p_value}

    return results


def neighborhood_comparison_tables(df, variables=None):
    """Calculates mean and standard deviation for the given variables in the DataFrame
    `df`, by grouping them based on the values in the `neighborhood` column.

    Args:
        df (pandas.DataFrame): A DataFrame containing the variables of interest.
        variables (list): A list of variable names to compute statistics for.

    Returns:
        pandas.DataFrame: A DataFrame containing the mean and standard deviation for each variable
        in each neighborhood.

    """
    if variables is None:
        variables = [
            "av_age",
            "female_rate",
            "ownership_rate",
            "overcrowd_rate",
            "unmet_basic_needs_rate",
            "av_hh_head_schooling",
            "unemployment_rate",
            "av_hh_size",
        ]

    dfs = []
    for var in variables:
        var_df = df.groupby("neighborhood")[var].agg(["mean", "std"]).reset_index()
        var_df.columns = ["neighborhood", f"{var}_mean", f"{var}_std"]
        dfs.append(var_df)

    return pd.concat(dfs, axis=1)
