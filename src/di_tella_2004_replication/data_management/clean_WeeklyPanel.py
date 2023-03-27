"""Function(s) for cleaning the data set(s)."""


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


def _gen_rep_variables_fixedextension_we(
    df,
    var_cond_ext,
    range_loop=range(1, 40),
    original_value_var=0,
    final_value_var=1,
):
    """Generates a set of variables based on a fixed extension, and replaces their
    values in a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame in which to generate and replace variables.
        var_cond_ext (str): The name of the variable in df used to condition the replacement.
        range_loop (range or iterable): An iterable with the values of the extension to use for variable names.
            Default is range(1, 40).
        original_value_var (int or any): The initial value to set to each generated variable.
            Default is 0.
        final_value_var (int or any): The final value to set to the variables that match the condition var_cond_ext==i.
            Default is 1.

    Returns:
        pandas.DataFrame: The input DataFrame with the generated variables replaced according to the condition.

    """
    for i in range_loop:
        list_names_ext = []
        list_names_ext.extend([f"week{i}"])
        df[list(list_names_ext)] = original_value_var  # generate
    for i in range_loop:
        df.loc[df[var_cond_ext] == i, f"week{i}"] = final_value_var  # replace
    return df


def _gen_rep_variables_fixedlistsimple_we(
    df,
    list_fixed,
    var_cond_fix,
    var_fix,
    cond_fix=2,
    value_var_fix_ori=0,
    value_var_fix_fin=1,
):
    """Generates a set of variables based on a fixed list, and replaces their values in
    a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame in which to generate and replace variables.
        list_fixed (list): A list of column names to generate.
        var_cond_fix (str): The name of the variable in df used to condition the replacement.
        var_fix (str): The name of the variable in df to replace its values.
        cond_fix (int or any): The condition to match in var_cond_fix to apply the replacement.
            Default is 2.
        value_var_fix_ori (int or any): The initial value to set to each generated variable.
            Default is 0.
        value_var_fix_fin (int or any): The final value to set to the variable var_fix that match the condition.
            Default is 1.

    Returns:
        pandas.DataFrame: The input DataFrame with the generated variables replaced according to the condition.

    """
    df[list(list_fixed)] = value_var_fix_ori  # generate
    df.loc[df[var_cond_fix] == cond_fix, var_fix] = value_var_fix_fin  # replace
    return df


def _rep_variables_we(
    df,
    type_of_condition,
    var_cond_rep,
    replace_var,
    condition_num=18,
    value_replace=1,
):
    """Generates a set of variables based on a fixed list, and replaces their values in
    a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame in which to generate and replace variables.
        list_fixed (list): A list of column names to generate.
        var_cond_fix (str): The name of the variable in df used to condition the replacement.
        var_fix (str): The name of the variable in df to replace its values.
        cond_fix (int or any): The condition to match in var_cond_fix to apply the replacement.
            Default is 2.
        value_var_fix_ori (int or any): The initial value to set to each generated variable.
            Default is 0.
        value_var_fix_fin (int or any): The final value to set to the variable var_fix that match the condition.
            Default is 1.

    Returns:
        pandas.DataFrame: The input DataFrame with the generated variables replaced according to the condition.

    """
    if type_of_condition == "bigger than":
        df.loc[df[var_cond_rep] > condition_num, replace_var] = value_replace
        return df
    elif type_of_condition == "smaller than":
        df.loc[df[var_cond_rep] < condition_num, replace_var] = value_replace
        return df
    elif type_of_condition == "equal to":
        df.loc[df[var_cond_rep] > condition_num, replace_var] = value_replace
        return df


def _gen_rep_variables_fixedlistcomplex_we(
    df,
    list1_fix_com,
    list2_fix_com,
    var_fix_comp_mul,
    list_rep_fix_com,
    var_fix_com_to_use="neighborhood",
    var_fix_com_to_change="n_neighborhood",
    range_rep_fix_com=range(1, 4),
):
    """This function generates new variables based on existing variables in a dataframe
    and replaces the values of a specified variable based on certain conditions.

    Args:
    - df: pandas dataframe
    - list1_fix_com: list of column names in the dataframe to be multiplied with `var_fix_comp_mul` to generate new variables
    - list2_fix_com: list of column names in the dataframe to be used in the multiplication
    - var_fix_comp_mul: variable to be used as a multiplier in the multiplication
    - list_rep_fix_com: list of values of `var_fix_com_to_use` to be replaced in `var_fix_com_to_change`
    - var_fix_com_to_use: name of the column in the dataframe to use as condition for replacement (default: 'neighborhood')
    - var_fix_com_to_change: name of the column in the dataframe to be replaced (default: 'n_neighborhood')
    - range_rep_fix_com: range of values to use for replacement (default: range(1, 4))

    Returns:
    - df: pandas dataframe with new variables generated and specified variable replaced based on conditions

    """
    for col1, col2 in zip(list1_fix_com, list2_fix_com):
        df[col1] = df[col2] * df[var_fix_comp_mul]
    for col, i in zip(list_rep_fix_com, range_rep_fix_com):
        df.loc[df[var_fix_com_to_use] == col, var_fix_com_to_change] = i
    return df


def weeklypanel(
    df,
    var_cond_ext,
    list_fixed,
    var_cond_fix,
    var_fix,
    type_of_condition,
    var_cond_rep,
    replace_var,
    list1_fix_com,
    list2_fix_com,
    var_fix_comp_mul,
    list_rep_fix_com,
    location,
    list_drop=[
        "street",
        "street_nr",
        "public_building_or_embassy",
        "gas_station",
        "bank",
    ],
    new_var_d="jewish_int_one_block_away_1",
    var1_d="jewish_inst_one_block_away",
    var2_d="jewish_inst",
    new_var_s="code2",
    var1_s="week",
    var2_s="n_neighborhood",
    new_var_sim="n_total_thefts",
    var_sim="total_thefts",
):
    df = _clean_column_names_we(df)
    df.drop(columns=list_drop, inplace=True)
    df = _gen_rep_variables_fixedextension_we(df, var_cond_ext)
    df = _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, var_fix)
    df = _rep_variables_we(df, type_of_condition, var_cond_rep, replace_var)
    df[new_var_d] = df[var1_d] - df[var2_d]
    df[new_var_s] = 1 * df[var1_s] + 1000 * df[var2_s]
    df[new_var_sim] = df[var_sim] * ((365 / 12) / 7)
    df = _gen_rep_variables_fixedlistcomplex_we(
        df,
        list1_fix_com,
        list2_fix_com,
        var_fix_comp_mul,
        list_rep_fix_com,
    )
    df.to_csv(location)
