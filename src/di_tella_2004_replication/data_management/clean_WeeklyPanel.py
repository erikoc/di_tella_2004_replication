"""Function(s) for cleaning the data set(s)."""
import pandas as pd

def _clean_column_names_we(df):
    """This function takes a pandas DataFrame and standardizes the column names to a
    specified format.

    The function renames the columns by replacing certain substrings with standardized terms such as "rob" with "theft",
    "day" with "week_day", "dia" with "day", "mes" with "month", "hor" with "hour", "mak" with "brand", and "esq" with "corner".

    In addition, the function replaces specific column names with more meaningful and descriptive names, as specified in the
    'replacements' dictionary.

    Parameters:
    df (pandas.DataFrame): The pandas DataFrame containing the data to be standardized.

    Returns:
    pandas.DataFrame: The input DataFrame with the columns standardized to the specified format.

    """
    df.columns = (
        df.columns.str.replace("observ", "observ")
        .str.replace("barrio", "neighborhood")
        .str.replace("calle", "street")
        .str.replace("altura", "street_nr")
        .str.replace("institu1", "jewish_inst")
        .str.replace("institu3", "jewish_inst_one_block_away")
        .str.replace("distanci", "distance_to_jewish_inst")
        .str.replace("edpub", "public_building_or_embassy")
        .str.replace("estserv", "gas_station")
        .str.replace("banco", "bank")
        .str.replace("totrob", "total_thefts")
        .str.replace("week", "week")
    )

    return df


def _generate_dummy_variables(df, variable_dummy):
    """Generate dummy variables from a categorical variable in a Pandas DataFrame.

    Args:
    df (pandas.DataFrame): The DataFrame containing the variable to be converted into dummy variables.
    variable (str): The name of the categorical variable to be converted into dummy variables.

    Returns:
    pandas.DataFrame: The input DataFrame with an additional column containing the original variable, and with new columns corresponding to the dummy variables.

    """

    df[f"{variable_dummy}_dummy"] = df[variable_dummy]
    df = pd.get_dummies(df, columns=[f"{variable_dummy}_dummy"], drop_first=False)
    return df


def _gen_new_variables():
def _generate_variables_from_list(
    df,
    list_fixed,
    conditional_variable,
    variable_to_change,
    conditional_variable_value=2,
    original_value_list=0,
    final_value=1,
):
    """Generates a set of variables based on a fixed list, and replaces their values in
    a Pandas DataFrame based on a condition.

    Args:
    df (pandas.DataFrame): The input DataFrame in which to generate and replace variables.
    list_fixed (list): A list of column names to generate in the DataFrame.
    conditional_variable (str): The name of the variable used as a condition to replace values in `variable_to_change`.
    variable_to_change (str): The name of the variable in `df` whose values are to be replaced based on the condition.
    conditional_variable_value (int or any, optional): The value of `conditional_variable` that triggers the replacement. Defaults to 2.
    original_value_list (int or any, optional): The initial value assigned to the generated variables in `list_fixed`. Defaults to 0.
    final_value (int or any, optional): The value to assign to `variable_to_change` when the `conditional_variable` matches `conditional_variable_value`. Defaults to 1.

    Returns:
    pandas.DataFrame: The input DataFrame with the generated variables replaced according to the condition.

    """
    df[list(list_fixed)] = original_value_list  # generate
    df.loc[df[conditional_variable] == conditional_variable_value, variable_to_change] = final_value  # replace
    return df




def _rep_variables_based_on_condition(
    df,
    type_of_condition,
    conditional_variable_replace,
    variable_to_replace,
    conditional_number=18,
    final_value_replace=1,
):
    """Replaces values of a variable in a Pandas DataFrame based on a condition.

    Args:
    df (pandas.DataFrame): The DataFrame in which to replace variables.
    type_of_condition (str): The type of condition to match. Can be "bigger than", "smaller than", or "equal to".
    conditional_variable_replace (str): The name of the variable in `df` used as a condition for the replacement.
    varaible_to_replace (str): The name of the variable in `df` whose values are to be replaced based on the condition.
    conditional_number (int or any, optional): The value of `conditional_variable_replace` to match with the condition. Defaults to 18.
    final_value_replace (int or any, optional): The value to replace the values of `variable_to_replace` when the condition is met. Defaults to 1.

    Returns:
    pandas.DataFrame: The input DataFrame with the variable values replaced according to the condition.

    """
    if type_of_condition == "bigger than":
        df.loc[df[conditional_variable_replace] > conditional_number, variable_to_replace] = final_value_replace
        return df
    elif type_of_condition == "smaller than":
        df.loc[df[conditional_variable_replace] < conditional_number, variable_to_replace] = final_value_replace
        return df
    elif type_of_condition == "equal to":
        df.loc[df[conditional_variable_replace] == conditional_number, variable_to_replace] = final_value_replace
        return df




def _generate_variables_from_list_complex(
    df,
    list1,
    list2,
    fixed_variable,
    list_for_replace,
    fixed_variable_to_use="neighborhood",
    fixed_variable_to_change="n_neighborhood",
    range_of_replace=range(1, 4),
):
    """Replaces values of a variable in a Pandas DataFrame based on a condition.

    Args:
    df (pandas.DataFrame): The DataFrame in which to replace variables.
    type_of_condition (str): The type of condition to match. Can be "bigger than", "smaller than", or "equal to".
    conditional_variable_replace (str): The name of the variable in `df` used as a condition for the replacement.
    varaible_to_replace (str): The name of the variable in `df` whose values are to be replaced based on the condition.
    conditional_number (int or any, optional): The value of `conditional_variable_replace` to match with the condition. Defaults to 18.
    final_value_replace (int or any, optional): The value to replace the values of `variable_to_replace` when the condition is met. Defaults to 1.

    Returns:
    pandas.DataFrame: The input DataFrame with the variable values replaced according to the condition.

    """
    for col1, col2 in zip(list1, list2):
        df[col1] = df[col2] * df[fixed_variable]
    for col, i in zip(list_for_replace, range_of_replace):
        df.loc[df[fixed_variable_to_use] == col, fixed_variable_to_change] = i
    return df


def weeklypanel(
    df,
    variable_dummy,
    list_fixed,
    conditional_variable,
    variable_to_change,
    type_of_condition,
    conditional_variable_replace,
    variable_to_replace,
    list1,
    list2,
    fixed_variable,
    list_for_replace,
    list_drop,
    location,
    new_variable_diff="jewish_int_one_block_away_1",
    var1_d="jewish_inst_one_block_away",
    var2_d="jewish_inst",
    new_variable_sum="code2",
    var1_s="week",
    var2_s="n_neighborhood",
    new_variable_simple="n_total_thefts",
    var_sim="total_thefts",

):
    df = _clean_column_names_we(df)
    df.drop(columns=list_drop, inplace=True)
    df = _generate_dummy_variables(df, variable_dummy)
    df = _generate_variables_from_list(df, list_fixed, conditional_variable, variable_to_change)
    df = _rep_variables_based_on_condition(df, type_of_condition, conditional_variable_replace, variable_to_replace)
    df[new_variable_diff] = df[var1_d] - df[var2_d]
    df[new_variable_sum] = 1 * df[var1_s] + 1000 * df[var2_s]
    df[new_variable_simple] = df[var_sim] * ((365 / 12) / 7)
    df = _generate_variables_from_list_complex(
        df,
        list1,
        list2,
        fixed_variable,
        list_for_replace,
    )
    df.to_csv(location)
