"""Function(s) for cleaning the data set(s)."""
import pandas as pd


def _clean_column_names_mon(df):
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
        .str.replace("mes", "month")
    )

    return df


def _generate_dummy_variables_fixed_extension(
    df,
    variable_conditional_extension,
    variable_name,
    range_loop,
    original_value_variable=0,
    final_value_variable=1,
):
    """This function is just generating new variables for our dataframe {df} based on a
    list extension which has a range of extension {range_loop}. The extension of the
    list follows a condition of extension with a format(i). In the end we give a value
    to all these variables {original_value_variable}. After that we replace the values
    of the variable given the condition that another variable.

    {variable_conditional_extension} in the data frame has a certain value "i" which is part of a loop range {range_loop}.
    The columns to be replaced are also the same as the extension condition that we used to extend the list to generate the variables.
    In the end, all the columns have a fixed replaced variable {final_value_variable}

    Args:
    - df: pandas dataframe
    - variable_conditional_extension: name of the column in the dataframe to use as condition for replacing the variable
    - variable_name: name of the variable to generate and replace
    - range_loop: range of values to use for generating and replacing the variable (default: range(5, 13))
    - original_value_var: value to be assigned to the generated variable (default: 0)
    - final_value_var: value to replace the variable with (default: 1)

    Returns:
    - df: pandas dataframe with new variables generated and specified variable replaced based on conditions

    """
    for i in range_loop:
        list_names_ext = [variable_name.format(i)]
        df[list(list_names_ext)] = original_value_variable
        df.loc[
            df[variable_conditional_extension] == i,
            variable_name.format(i),
        ] = final_value_variable
    return df


def _rep_variables_based_on_condition(
    df,
    type_of_condition,
    conditional_variable_replace,
    variable_to_replace,
    conditional_number=7,
    final_value_replace=1,
):
    """This function is replacing variables based on a condition with three possible
    values {"bigger than", "smaller than", "equal to"}. Depending on the condition
    chosen, a variable {variable_to_replace} in the data frame {df} is replaced with the
    condition that another variable in the data frame {conditional_variable_replace} has
    a certain value {conditional_number}. In the end, the variable {variable_to_replace}
    gets a final value {final_value_replace} if the condition was accomplished.

    Replaces values of a variable in a Pandas DataFrame based on a condition.

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
        df.loc[
            df[conditional_variable_replace] > conditional_number,
            variable_to_replace,
        ] = final_value_replace
        return df
    elif type_of_condition == "smaller than":
        df.loc[
            df[conditional_variable_replace] < conditional_number,
            variable_to_replace,
        ] = final_value_replace
        return df
    elif type_of_condition == "equal to":
        df.loc[
            df[conditional_variable_replace] == conditional_number,
            variable_to_replace,
        ] = final_value_replace
        return df


def _generate_similar_named_variables(
    df,
    original_variables,
    fixed_variable,
    name_change="p",
):
    """This functions is generating a list of variables that have a quite similar name
    compared to an already existing set of variables.

    {original_variables} in a dataframe {df} and it is generated by giving them a similar name with a new added condition to the original ones
    {name_change} and by multiplying the original columns of the data frame on a loop by a fixed column {fixed_variable} in the dataframe

    Args:
    - df: pandas dataframe
    - original_variables: list of column names in the dataframe to generate new variables from
    - fixed_variable: variable to be used as a multiplier in the multiplication
    - name_change: string to be added to the end of each new variable name (default: 'p')

    Returns:
    - df: pandas dataframe with new variables generated

    """
    for col in original_variables:
        df[col + name_change] = df[col] * df[fixed_variable]
    return df


def _generate_variables_based_on_list_and_loop(
    df,
    condition_type,
    new_generated_variable,
    list_a,
    list_b,
    new_original_value,
    value_originallist=1,
):
    """Generates a new variable {new_generated_variable} with an original value.

    {new_original_value} in a pandas DataFrame {df} based on conditions.

    {"condition from another variable", "condition from original variable"} on a list of variables {list_a} and a range of values {list_b},
    replacing the values of the new variable with different values based on a loop.

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame to generate new variables in.
    condition_type : str or tuple
        The type of condition to apply for replacing values of the new variable.
        Possible values are "condition from another variable" and "condition from original variable".
    new_generated_variable : str
        The name of the new generated variable.
    list_a : list of str or range
        The list of variables in the DataFrame to apply conditions on, or a range of values for a specific condition.
    list_b : list or range
        The range of values to replace the new generated variable with based on the conditions.
    new_original_value : int, optional (default=4)
        The original value to initialize the new generated variable.
    range_new_gen : range object, optional (default=range(1,4))
        The range of values to replace the new generated variable with based on the conditions.
    value_originallist : int, optional (default=1)
        The condition for the original variables to meet for the new generated variable to be replaced with a
        value from the range, in the case of "condition from another variable".

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the new generated variable added based on the conditions.

    """
    if isinstance(condition_type, tuple):
        condition_type = condition_type[0]

    if condition_type == "condition from another variable":
        df[
            new_generated_variable
        ] = new_original_value  # in this case new_original_value is a number
        for col, i in zip(
            list_a,
            list_b,
        ):  # first list is a list of variables, second is a range
            df.loc[
                df[col] == value_originallist,
                new_generated_variable,
            ] = i  # we need a variable original list
        return df
    if condition_type == "condition from original variable":
        df[new_generated_variable] = df[
            new_original_value
        ]  # in this case new_original_value is a column of the dataframe
        for i, j in zip(
            list_a,
            list_b,
        ):  # first list is a range of number, second is also one
            df.loc[df[new_original_value] == i, new_generated_variable] = j
        return df


def _generate_variable_basedon_doublelist(
    df,
    list_generated_variable,
    list_original_variable,
    fixed_variable,
):
    """This function generates new variables listed on a list {list_generated_variable}
    in a dataframe {df} using a multiplication rule by a fixed variable {fixed_variable}
    that already exists in the data frame by another list of variables already present
    in the data frame {list_original_variable}.

    Args:
    - df (pandas.DataFrame): the dataframe where the new variables will be created
    - list_generated_variable (list): a list of strings representing the names of the new variables to be generated
    - list_original_variable (list): a list of strings representing the names of the existing variables used to calculate the values of the new variables
    - fixed_variable (str): the name of an existing variable in the dataframe used to multiply the values of the original variables

    Returns:
    - df (pandas.DataFrame): the input dataframe with new columns added corresponding to the generated variables

    """
    for col1, col2 in zip(list_generated_variable, list_original_variable):
        df[col1] = df[col2] * df[fixed_variable]
    return df


def _generate_total_thefts2_mon(df, variable_complex_condition, cond1=72, cond2=73):
    """This function is generating a a new variable by observation "total_thefts2" in a
    dataframe (df).

    Args:
    df (pandas DataFrame): The input dataframe.
    varaible_complex_cond (str): The name of the column which contains the condition for selecting the cumulative sum.
    cond1 (int or float): The first value used for selecting the cumulative sum (default=72).
    cond2 (int or float): The second value used for selecting the cumulative sum (default=73).

    Returns:
    pandas DataFrame: The input dataframe with a new column "total_thefts2" added, which contains the calculated values based on the given condition.

    """
    df = df.assign(total_thefts2=pd.Series())
    df["total_thefts2"] = df["total_thefts2"].tolist()
    for i in range(1, len(df)):
        if df[variable_complex_condition].iloc[i] in [cond1, cond2]:
            df["total_thefts2"].loc[i] == df["total_thefts"].iloc[i].cumsum()
    df.loc[
        (df[variable_complex_condition] != cond1)
        & (df[variable_complex_condition] != cond2),
        "total_thefts2",
    ] = df["total_thefts"]
    df["total_thefts2"] = pd.Series(df["total_thefts2"])
    return df


def _generate_variables_different_conditions(
    df,
    new_variable_v_cond,
    ori_variable_v_cond,
    variable_conditional_v_cond,
    condition_v1=5,
    condition_v2=7,
    condition_v3=8,
    condition_v4=10,
    condition_v5=12,
    multiple1_v_cond=None,
    multiple2_v_cond=None,
):
    """Generates a new variable {new_variable_v_cond} in a dataframe based on the values
    of another existing variable {ori_variable_v_cond} and a condition on a variable.

    {variable_conditional_v_cond} in the data frame {df}.

    Args:
    - df (pandas.DataFrame): The input dataframe.
    - new_variable_v_cond (str): The name of the new variable to be generated in the dataframe.
    - ori_variable_v_cond (str): The name of the existing variable that will be used to generate the new variable.
    - variable_conditional_v_cond (str): The name of the variable that will be used to apply the condition on the dataframe.
    - condition_v1 (int): The first value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 7.
    - condition_v2 (int): The second value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 5.
    - condition_v3 (int): The third value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 8.
    - condition_v4 (int): The fourth value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 10.
    - condition_v5 (int): The fifth value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 12.
    - multiple1_v_cond (float): The scaling factor for the generated variable when `var_con_v_cond` equals `con_v_cond1`. Default value is 30/17.
    - multiple2_v_cond (float): The scaling factor for the generated variable when `var_con_v_cond` equals any of `con_v_cond2`, `con_v_cond3`, `con_v_cond4`, or `con_v_cond5`. Default value is 30/31.

    Returns:
    - pandas.DataFrame: A new dataframe with the new variable added and values of the new variable generated based on the specified conditions.

    """
    if multiple1_v_cond is None:
        multiple1_v_cond = 30 / 17

    if multiple2_v_cond is None:
        multiple2_v_cond = 30 / 31

    df[new_variable_v_cond] = df[ori_variable_v_cond]
    df.loc[df[variable_conditional_v_cond] == condition_v1, new_variable_v_cond] = (
        df[ori_variable_v_cond] * multiple1_v_cond
    )
    df.loc[
        (df[variable_conditional_v_cond] == condition_v2)
        | (df[variable_conditional_v_cond] == condition_v3)
        | (df[variable_conditional_v_cond] == condition_v4)
        | (df[variable_conditional_v_cond] == condition_v5),
        new_variable_v_cond,
    ] = (
        df[ori_variable_v_cond] * multiple2_v_cond
    )
    return df


def _generate_variables_original_with_no_value_after_replace(
    df,
    list_variables_original_novalue,
    conditional_variable_for_novalue,
    equalizing_variable,
    condition_change_value=7,
):
    """Generates a set of variables with no values {list_variables_original_novalue} in
    a dataframe {df}. The value of some of the variables on the list are replaced with
    values of an original variable in the dataframe {equalizing_variable} if a condition
    for another variable {conditional_variable_for_novalue} in the data frame is met.

    {condition_change_value}.

    Args:
        df (pandas.DataFrame): The input dataframe.
        list_variables_original_novalue, (list): A list of variable names to generate NAs.
        conditional_variable_for_novalue (str): The name of the variable that is used to apply conditions on whether to replace values or not.
        equalizing_variable (str): The name of the variable used to replace the values in the list_for_NA.
        condition_change_value (int): The fixed value to compare the variable var_con_NA against. Default is 7.

    Returns:
        pandas.DataFrame: The dataframe with new variables generated with NAs and some variables replaced
        based on the specified conditions.

    """
    for col in list_variables_original_novalue:
        df[col] = pd.NA
    df.loc[
        df[conditional_variable_for_novalue] < condition_change_value + 1,
        list_variables_original_novalue[0],
    ] = df[equalizing_variable]
    df.loc[
        df[conditional_variable_for_novalue] > condition_change_value,
        list_variables_original_novalue[1],
    ] = df[equalizing_variable]
    return df


def _egenerator_sum(
    df,
    new_egenerator_variable,
    by_variable,
    variable_egenerator_filter,
    condional_egenerator_variable,
    egenerator_variable_tochange,
    conditon_egenerator_value=4,
    egenerator_scale_factor=4,
):
    """Generates a new variable {new_egenerator_variable} in a dataframe {df} based on
    the sum of an existing variable {by_variable} filtered by.

    {variable_egenerator_filter}. Then, replaces the values of an existing variable
    {egenerator_variable_tochange} in the data frame with the new variable
    {new_egenerator_variable} scaled by a given factor {egenerator_scale_facto} if an
    existing variable.

    {condional_egenerator_variable} meets a certain condition {conditon_egenerator_value}.

    Args:
        df (pandas.DataFrame): The input dataframe.
        new_egenerator_variable (str): The name of the new variable to be generated.
        by_variable (str): The name of the by variable to group the sum of an existing variable.
        variable_egenerator_filter (str): The name of the existing variable to take the sum of.
        condional_egenerator_variable (str): The name of the existing variable to check the condition for replacement.
        egenerator_variable_tochange (str): The name of the existing variable to be replaced.
        conditon_egenerator_value (int, optional): The condition value for replacement. Defaults to 4.
        egenerator_scale_factor (int, optional): The scaling factor for the new variable. Defaults to 4.

    Returns:
        pandas.DataFrame: The modified dataframe.

    """
    df[new_egenerator_variable] = df.groupby(by_variable)[
        variable_egenerator_filter
    ].transform("sum")
    df.loc[
        df[condional_egenerator_variable] == conditon_egenerator_value,
        egenerator_variable_tochange,
    ] = (
        df[new_egenerator_variable] / egenerator_scale_factor
    )
    return df


def _complex_variable_generator(
    df,
    list_names_complexa,
    variable_replace_condition_complex,
    variable_replace_complex,
    generate_var_complex,
    variable_condition_complex,
    list_names_complexb,
    scale_complex=1000,
    range_complex=range(1, 4),
    final_value_complex=1,
):
    """Replaces the values of a variable { variable_replace_complex} in a dataframe {df}
    based on a condition on another variable.

    {variable_replace_condition_com}. Adter that, it generates a new variable {generate_var_complex}
    based on two existing variables {variable_condition_complex, variable_replace_complex}, one of them scaled by a factor {scale_complex},
    and generates two new variables based on a list of names {list_names_complexa, list_names_complexb}, and replaces their values
    based on a condition on an existing variable {variable_replace_condition_complex} which is based on the loops of the lists.

    Parameters:
    -----------
    df : pandas DataFrame
        The input dataframe that contains the variables to be manipulated.
    list_names_complex : list
        A list of strings containing the names to be used in the replacement of var_rep_complex based on
        var_rep_cond_complex.
    variable_replace_condition_complex : str
        The name of the existing variable used as a condition to replace the values in var_rep_complex.
    variable_replace_complex : str
        The name of the variable whose values will be replaced.
    generate_var_complex : str
        The name of the new generated variable to be added to the dataframe.
    variable_condition_complex : str
        The name of the variable to be added to gen_var_complex.
    list_names_complexb : list
        A list of strings containing the names to be used in the replacement of the new generated variables.
    scale_complex : int, optional
        The scalar factor to be applied to var_rep_complex when generating gen_var_complex. Default is 1000.
    range_complex : range, optional
        The range of values to be used in the replacement of var_rep_complex based on var_rep_cond_complex.
        Default is range(1, 4).
    final_value_complex : int, optional
        The final value to be applied to the new generated variables. Default is 1.

    Returns:
    --------
    df : pandas DataFrame
        The dataframe with the new generated variables and the modified values of the existing ones.

    """
    for col, i in zip(list_names_complexa, range_complex):
        df.loc[
            df[variable_replace_condition_complex] == col,
            variable_replace_complex,
        ] = i
    df[generate_var_complex] = (
        df[variable_condition_complex] + scale_complex * df[variable_replace_complex]
    )
    for col1, col2 in zip(list_names_complexb, list_names_complexa):
        df[col1] = 0
        df.loc[
            df[variable_replace_condition_complex] == col2,
            col1,
        ] = final_value_complex
    return df


def _generate_variables_based_on_various_lists(
    df,
    list_names_variouslists,
):
    """This function is firstly extending two lists (list_names_place, list_names_month)
    which in turn by using a fixed loop and using the multiplication rule, will be used
    to create new variables on a list {list_names_variouslists}.

    Args:
    - df: Pandas DataFrame to generate the variables on.
    - list_names_variouslists: List of variable names to be multiplied to generate new variables.

    Returns:
    - df: Pandas DataFrame with the newly created variables.

    """
    list_names_place = [
        f"mbelg{i}"
        for i in [
            "apr",
            "may",
            "jun",
            "jul",
            "ago",
            "sep",
            "oct",
            "nov",
            "dec",
        ]
    ]
    list_names_place.extend(
        [
            f"monce{i}"
            for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]
        ],
    )
    list_names_place.extend(
        [
            f"mvcre{i}"
            for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]
        ],
    )

    list_names_month = [f"month{i}" for i in range(4, 13)]
    """This is making a loop over the three different elements of
    list_names_variouslists_m2.

    Ity is usimng an index to go through thje 27 elements in list_names_place and it is
    using a second loop to go over the elemensr of list_names_month.

    """
    for i in range(3):
        start_idx = i * 9
        for j in range(9):
            col1 = f"{list_names_place[start_idx + j]}"
            col2 = list_names_variouslists[i]
            col3 = list_names_month[j]
            df[col1] = df[col2] * df[col3]
    return df


def _generate_variable_based_on_three_or_conditions(
    df,
    generate_variable_three_conditions,
    column1_three_conditions,
    column2_three_conditions,
    column3_three_conditions,
    initial_value_three_conditions=0,
    global_replace_value_three_conditions=1,
):
    """This function is generating a variable {generate_variable_three_conditions} in a
    dataframe {df} with an initial value.

    {initial_value_three_conditions}. Then it is replacing the value of this variable if any of three condition on three variables are met
    {column1_three_conditions, column2_three_conditions, column3_three_conditions}.
    The values need to meet a condition which in turns is also the final value of the generated variable {global_replace_value_three_conditions}

    Args:
    - df: pandas DataFrame to work on.
    - gen_var_3cond (str): name of the new variable to generate.
    - col1_3cond (str): name of the first column to consider for the conditions.
    - col2_3cond (str): name of the second column to consider for the conditions.
    - col3_3cond (str): name of the third column to consider for the conditions.
    - initial_val_3cond (int): initial value for the new variable (default=0).
    - global_replace_val_3cond (int): value to use when replacing the new variable (default=1).

    Returns:
    - pandas DataFrame with the new variable generated and potentially replaced.

    """
    df[generate_variable_three_conditions] = initial_value_three_conditions
    df.loc[
        (df[column1_three_conditions] == global_replace_value_three_conditions)
        | (df[column2_three_conditions] == global_replace_value_three_conditions)
        | (df[column3_three_conditions] == global_replace_value_three_conditions),
        generate_variable_three_conditions,
    ] = global_replace_value_three_conditions
    return df


def _generate_multiplevariables_listbased(
    df,
    list_names_multi_variables,
    list_names_multi_general,
):
    """This function is list based. Given some list entries. Given an inbuilt list.

    {list_value}, it generates different values based on a multiplication rule with for
    two nested loops using two lists {list_names_multi_variables,
    list_names_multi_general}.

    Parameters:
    -----------
    df : pandas.DataFrame
        The dataframe to which variables will be added.
    list_values : list
        A list of values for each variable to be created. Each element of the list should be a list of length 6 containing
        three names for variables to be created using multiplication of columns from the dataframe, and three names for
        variables to be created using the complement (1-x) of one of the multiplication variables and another column from
        the dataframe.
    list_names_multi_variables : list
        A list of names of columns from the dataframe to be used for the first three variables of each element in list_values.
    list_names_multi_general : list
        A list of names of columns from the dataframe to be used for the last three variables of each element in list_values.

    Returns:
    --------
    pandas.DataFrame
        The input dataframe with additional columns added based on the values in the input lists.

    """
    list_value1_m3 = [
        "public_building_or_embassy_p",
        "public_building_or_embassy_1_p",
        "public_building_or_embassy_cuad2p",
        "n_public_building_or_embassy_p",
        "n_public_building_or_embassy_1_p",
        "n_public_building_or_embassy_cuad2p",
    ]
    list_value2_m3 = [
        "gas_station_p",
        "gas_station_1_p",
        "gas_station_cuad2p",
        "n_gas_station_p",
        "n_gas_station_1_p",
        "n_gas_station_cuad2p",
    ]
    list_value3_m3 = [
        "bank_p",
        "bank_1_p",
        "bank_cuad2p",
        "n_bank_p",
        "n_bank_1_p",
        "n_bank_cuad2p",
    ]
    list_value4_m3 = [
        "all_locations_p",
        "all_locations_1_p",
        "all_locations_cuad2p",
        "n_all_locations_p",
        "n_all_locations_1_p",
        "n_all_locations_cuad2p",
    ]

    list_values = [list_value1_m3, list_value2_m3, list_value3_m3, list_value4_m3]

    for i, values in enumerate(list_values):
        for j in range(3):
            df[values[j]] = (
                df[list_names_multi_variables[i]] * df[list_names_multi_general[j]]
            )
        for j in range(3, 6):
            df[values[j]] = (1 - df[list_names_multi_variables[i]]) * df[
                list_names_multi_general[j]
            ]
    return df


def _generate_various_variables_conditional(
    df,
    list_genenerate_variables_conditional,
    conditional_variable,
    original_value_conditional=0,
    range_variable_conditional=range(4, 7),
    final_value_conditional=1,
):
    """Generates a set of new variables from a list.

    {list_genenerate_variables_conditional} in a data frame {df} with an original value.

    {original_value_conditional}. After that, using the numbers of elements of the list and also a range {range_variable_conditional}
    for a loop, the values of these new variables are replaced if the condition of another existing variable on the data frame is met
    {conditional_variable} and if the condition is met, it is given a final value {final_value_conditional}

    Args:
        df (pandas.DataFrame): The input dataframe.
        list_genenerate_variables_conditional (list): A list of variables to generate.
        conditional_variable (str): The name of the column containing the condition.
        original_value_conditional (int, optional): The original value to assign to the generated variables. Defaults to 0.
        range_variable_conditional (range, optional): A range object for the loop to generate and replace variables. Defaults to range(4,7).
        final_value_conditional (int, optional): The value to assign to the generated variables when the condition is met. Defaults to 1.

    Returns:
        pandas.DataFrame: A modified version of the input dataframe with the generated variables and the replacements.

    """
    for col in list_genenerate_variables_conditional:
        df[col] = original_value_conditional
    for col, i in zip(
        list_genenerate_variables_conditional,
        range_variable_conditional,
    ):
        df.loc[df[conditional_variable] > i, col] = final_value_conditional
    return df


def _generate_variables_specificrule_list(
    df,
    list_variable_generate_specific,
    list_variable_extisting_specific,
    list_new_variable_specific,
    range_specific_loop=[0, 3, 6],
):
    """This function is generating specific new variables.

    {list_variable_generate_specific} in a dataframe {df} based on an already existing
    set of variables in the data frame {list_variable_extisting_specific,
    list_new_variable_specific} and this is done over a loop specifically designed by
    use to be used here in this function with range {range_specific_loop}.

    Inputs:

    df: a pandas DataFrame
    list_variable_generate_specific: a list of new variable names to be generated in the DataFrame df
    list_variable_extisting_specific: a list of existing variable names in the DataFrame df to be used in the generation of new variables
    list_new_var_spec: a list of new variable names to be generated in the DataFrame df
    range_specific_loop: a list of integers representing the indices for generating the new variables

    Outputs:

    df: a pandas DataFrame with the newly generated variables based on specific rules

    """
    for i in range_specific_loop:
        for col1, col2, col3 in zip(
            list_variable_generate_specific,
            list_variable_extisting_specific,
            list_new_variable_specific[i : i + 3],
        ):
            df[col3] = df[col1] * df[col2]
    return df


def monthlypanel_1(
    df,
    original_variables=[
        "cuad0",
        "cuad1",
        "cuad2",
        "cuad3",
        "cuad4",
        "cuad5",
        "cuad6",
        "cuad7",
    ],
    fixed_variable="post",
    list_generated_variable=["jewish_inst_p", "jewish_inst_one_block_away_1_p"],
    list_original_variable=["jewish_inst", "jewish_inst_one_block_away_1"],
    variable_generated="post",
    original_value=0,
    type_of_condition="bigger than",
    conditional_variable_replace="month",
    variable_to_replace="post",
    conditional_number=7,
    final_value_replace=1,
    list_sort=["observ", "month"],
    new_variable="jewish_inst_one_block_away_1",
    var1="jewish_inst_one_block_away",
    var_sub="jewish_inst",
    variable_complex_condition="month",
):
    df = _clean_column_names_mon(df)
    df = _generate_dummy_variables_fixed_extension(
        df,
        variable_conditional_extension="month",
        variable_name="month{}",
        range_loop=range(5, 13),
    )
    df[new_variable] = df[var1] - df[var_sub]
    df[variable_generated] = original_value
    df = _rep_variables_based_on_condition(
        df,
        type_of_condition,
        conditional_variable_replace,
        variable_to_replace,
        conditional_number,
        final_value_replace,
    )
    df = _generate_dummy_variables_fixed_extension(
        df,
        variable_conditional_extension="distance_to_jewish_inst",
        variable_name="cuad{}",
        range_loop=range(8),
    )
    df = _generate_similar_named_variables(
        df,
        original_variables,
        fixed_variable,
    )
    df = _generate_variables_based_on_list_and_loop(
        df,
        condition_type="condition from another variable",
        new_generated_variable="code",
        list_a=["jewish_inst", "jewish_inst_one_block_away_1", "cuad2"],
        list_b=range(1, 4),
        new_original_value=4,
        value_originallist=1,
    )
    df = _generate_variable_basedon_doublelist(
        df,
        list_generated_variable,
        list_original_variable,
        fixed_variable,
    )
    df = _generate_variables_based_on_list_and_loop(
        df,
        condition_type="condition from original variable",
        new_generated_variable="othermonth1",
        list_a=[72, 73],
        list_b=[7.2, 7.3],
        new_original_value="month",
        value_originallist=1,
    )
    df = df.sort_values(list_sort)
    df = _generate_total_thefts2_mon(
        df,
        variable_complex_condition,
    )
    return df


def monthlypanel_2(
    df,
    var_drop="month",
    drop1=72,
    drop2=73,
    new_variable_v_cond="total_thefts_c",
    ori_variable_v_cond="total_thefts",
    variable_conditional_v_cond="month",
    list_variables_original_novalue=["prethefts", "posthefts", "theftscoll"],
    conditional_variable_for_novalue="month",
    equalizing_variable="total_thefts",
    list_sort=["observ", "month"],
    new_generated_var_sim="w",
    value_sim=0.25,
    new_generated_var_sim2="n_neighborhood",
    value_sim2=0,
    new_generated_var="total_thefts_q",
    existing_variable="total_thefts",
    scalar_gen=4,
    list_names_complexa=["Belgrano", "Once", "V. Crespo"],
    variable_replace_condition_complex="neighborhood",
    variable_replace_complex="n_neighborhood",
    generate_var_complex="code2",
    variable_condition_complex="month",
    list_names_complexb=["belgrano", "once", "vcrespo"],
    variable_gen_simple="month4",
    original_val_simple=0,
    type_of_condition="equal to",
    conditional_variable_replace="month",
    variable_to_replace="month4",
    list_names_variouslists=["belgrano", "once", "vcrespo"],
):

    df.drop(
        df.loc[(df[var_drop] == drop1) | (df[var_drop] == drop2)].index,
        inplace=True,
    )
    df = _generate_variables_different_conditions(
        df,
        new_variable_v_cond,
        ori_variable_v_cond,
        variable_conditional_v_cond,
    )
    df = _generate_variables_original_with_no_value_after_replace(
        df,
        list_variables_original_novalue,
        conditional_variable_for_novalue,
        equalizing_variable,
    )
    df = df.sort_values(list_sort)
    df = _egenerator_sum(
        df,
        new_egenerator_variable="totalpre",
        by_variable="observ",
        variable_egenerator_filter="prethefts",
        condional_egenerator_variable="month",
        egenerator_variable_tochange="theftscoll",
        conditon_egenerator_value=4,
        egenerator_scale_factor=4,
    )
    df = _egenerator_sum(
        df,
        new_egenerator_variable="totalpos",
        by_variable="observ",
        variable_egenerator_filter="posthefts",
        condional_egenerator_variable="month",
        egenerator_variable_tochange="theftscoll",
        conditon_egenerator_value=8,
        egenerator_scale_factor=5,
    )
    df[new_generated_var] = df[existing_variable] * scalar_gen
    df[new_generated_var_sim] = value_sim
    df[new_generated_var_sim2] = value_sim2
    df = _complex_variable_generator(
        df,
        list_names_complexa,
        variable_replace_condition_complex,
        variable_replace_complex,
        generate_var_complex,
        variable_condition_complex,
        list_names_complexb,
    )
    df[variable_gen_simple] = original_val_simple
    df = _rep_variables_based_on_condition(
        df,
        type_of_condition,
        conditional_variable_replace,
        variable_to_replace,
        conditional_number=4,
        final_value_replace=1,
    )
    df = _generate_variables_based_on_various_lists(
        df,
        list_names_variouslists,
    )
    return df


def monthlypanel_3(
    df,
    generate_variable_three_conditions="all_locations",
    column1_three_conditions="public_building_or_embassy",
    column2_three_conditions="gas_station",
    column3_three_conditions="bank",
    list_names_multi_variables=[
        "public_building_or_embassy",
        "gas_station",
        "bank",
        "all_locations",
    ],
    list_names_multi_general=[
        "jewish_inst_p",
        "jewish_inst_one_block_away_1_p",
        "cuad2p",
        "jewish_inst_p",
        "jewish_inst_one_block_away_1_p",
        "cuad2p",
    ],
    column_to_drop="month4",
):
    df.drop(columns=column_to_drop)
    df = _generate_variable_based_on_three_or_conditions(
        df,
        generate_variable_three_conditions,
        column1_three_conditions,
        column2_three_conditions,
        column3_three_conditions,
    )
    df = _generate_multiplevariables_listbased(
        df,
        list_names_multi_variables,
        list_names_multi_general,
    )
    return df


def monthlypanel_new(
    df,
    new_variable="jewish_inst_one_block_away_1",
    variable_original="jewish_inst_one_block_away",
    variable_original_substract="jewish_inst",
    list_various_generate=["cuad2", "month5", "month6", "month7"],
    original_value_various_list=0,
    conditional_var_simple="distance_to_jewish_inst",
    variable_simple_replace="cuad2",
    cond_value_simple=2,
    value_assigned_simple=1,
    range_replace=range(5, 8),
    conditional_variable="month",
    value_assigned_various=1,
    list_genenerate_variables_conditional=["post1", "post2", "post3"],
    list_variable_generate_specific=[
        "jewish_inst",
        "jewish_inst_one_block_away_1",
        "cuad2",
    ],
    list_variable_extisting_specific=["post1", "post2", "post3"],
    list_new_variable_specific=[
        "one_jewish_inst_1_p",
        "one_jewish_inst_one_block_away_1_p",
        "one_cuad2p",
        "two_jewish_inst_1_p",
        "two_jewish_inst_one_block_away_1_p",
        "two_cuad2p",
        "three_jewish_inst_1_p",
        "three_jewish_inst_one_block_away_1_p",
        "three_cuad2p",
    ],
):
    df = _clean_column_names_mon(df)
    df[new_variable] = df[variable_original] - df[variable_original_substract]
    df[list(list_various_generate)] = original_value_various_list
    df.loc[
        df[conditional_var_simple] == cond_value_simple,
        variable_simple_replace,
    ] = value_assigned_simple
    for i in range_replace:
        df.loc[df[conditional_variable] == i, f"month{i}"] = value_assigned_various
    df = _generate_various_variables_conditional(
        df,
        list_genenerate_variables_conditional,
        conditional_variable,
    )
    df = _generate_variables_specificrule_list(
        df,
        list_variable_generate_specific,
        list_variable_extisting_specific,
        list_new_variable_specific,
    )
    return df
