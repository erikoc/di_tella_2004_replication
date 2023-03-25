import pandas as pd
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_WeeklyPanel import (
    _clean_column_names_we,
)


@pytest.fixture()
def original_data():
    return {
        "monthly_panel": pyreadstat.read_dta(SRC / "data" / "MonthlyPanel.dta"),
        "weekly_panel": pyreadstat.read_dta(SRC / "data" / "WeeklyPanel.dta"),
    }


def test_clean_column_names_we(input_data_clean_column_names_we):
    # Call the function being tested
    new_df = _clean_column_names_we(original_data["weekly_panel"])

    # Define list of names replaced
    list_replace = [
        "observ",
        "neighborhood",
        "street",
        "street_nr",
        "jewish_inst",
        "jewish_inst_one_block_away",
        "distance_to_jewish_inst",
        "public_building_or_embassy",
        "gas_station",
        "bank",
        "total_thefts",
        "week",
    ]

    # Assert that the new columns are there
    assert all(list_replace) in new_df.columns


### For the new test this will be used --------------
WeeklyPanel = _clean_column_names_we(WeeklyPanel)
####-------------------------------------------------


def _gen_rep_variables_fixedextension_we(
    df,
    list_names_ext,
    range_ext,
    original_value_var,
    final_value_var,
    range_loop,
    var_cond_ext,
):
    """"This functions has certain inputs to generate a variable and replace its values
    (list_names_ext, range_ext, original_value_var, final_value_var, range_loop,
    var_cond_ext)"""
    for i in range_ext:
        list_names_ext.extend([f"week{i}"])
    df[list(list_names_ext)] = original_value_var  # generate
    for i in range_loop:
        df.loc[df[var_cond_ext] == i, f"week{i}"] = final_value_var  # replace
    return df


@pytest.fixture()
def input_data_gen_rep_variables_fixedextension_we():
    df = WeeklyPanel
    list_names_ext = ["week1"]
    range_ext = range(2, 40)
    original_value_var = 0
    final_value_var = 0
    range_loop = range(1, 40)
    var_cond_ext = "week"
    return (
        df,
        list_names_ext,
        range_ext,
        original_value_var,
        final_value_var,
        range_loop,
        var_cond_ext,
    )


def test_gen_rep_variables_fixedextension_we(
    input_data_gen_rep_variables_fixedextension_we,
):
    (
        df,
        list_names_ext,
        range_ext,
        original_value_var,
        final_value_var,
        range_loop,
        var_cond_ext,
    ) = input_data_gen_rep_variables_fixedextension_we

    # Call the function being tested
    new_df = _gen_rep_variables_fixedextension_we(
        df,
        list_names_ext,
        range_ext,
        original_value_var,
        final_value_var,
        range_loop,
        var_cond_ext,
    )

    # Test that the new columns were added with the expected values that I assigned to them
    for i in range_ext:
        assert f"week{i}" in new_df.columns
        assert all(new_df[f"week{i}"] == original_value_var)

    # Test that the values were replaced as we wanted
    for i in range_loop:
        assert all(new_df.loc[new_df[var_cond_ext] == i, f"week{i}"] == final_value_var)


def _gen_rep_variables_fixedlistsimple_we(
    df,
    list_fixed,
    var_cond_fix,
    cond_fix,
    var_fix,
    value_var_fix,
):
    """This functions has certain inputs to generate a variable and replace its values
    (list_fixed, var_cond_fix, var_fix, value_var_fix)"""
    df[list(list_fixed)] = 0  # generate
    df.loc[df[var_cond_fix] == cond_fix, var_fix] = value_var_fix  # replace
    return df


@pytest.fixture()
def input_data_gen_rep_variables_fixedlistsimple_we():
    df = WeeklyPanel
    list_fixed = ["cuad2", "post", "n_neighborhood"]
    var_cond_fix = "distance_to_jewish_inst"
    cond_fix = 2
    var_fix = "cuad2"
    value_var_fix = 1
    return df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix


def test_gen_rep_variables_fixedlistsimple_we(
    input_data_gen_rep_variables_fixedlistsimple_we,
):
    (
        df,
        list_fixed,
        var_cond_fix,
        cond_fix,
        var_fix,
        value_var_fix,
    ) = input_data_gen_rep_variables_fixedlistsimple_we

    # Call the function being tested
    new_df = _gen_rep_variables_fixedlistsimple_we(
        df,
        list_fixed,
        var_cond_fix,
        cond_fix,
        var_fix,
        value_var_fix,
    )

    # Test that the new columns were added
    assert all(list_fixed) in new_df.columns

    # Test that the values were replaced as we wanted
    assert all(
        new_df.loc[new_df[var_cond_fix] == cond_fix, list_fixed] == value_var_fix,
    )


def _rep_variables_we(
    df,
    type_of_condition,
    var_cond_rep,
    condition_num,
    replace_var,
    value_replace,
):

    """What this function does is just to replace a variable from a data frame depending
    on different types of conditions and with inputs (var_cond_rep, condition_num,
    replace_var, value_replace)"""

    if type_of_condition == "bigger than":
        df.loc[df[var_cond_rep] > condition_num, replace_var] = value_replace
        return df
    elif type_of_condition == "smaller than":
        df.loc[df[var_cond_rep] < condition_num, replace_var] = value_replace
        return df
    elif type_of_condition == "equal to":
        df.loc[df[var_cond_rep] > condition_num, replace_var] = value_replace
        return df


@pytest.fixture()
def input_data_rep_variables_we():
    df = WeeklyPanel
    type_of_condition = "bigger than"
    var_cond_rep = "week"
    condition_num = 18
    replace_var = "post"
    value_replace = 1
    return (
        df,
        type_of_condition,
        var_cond_rep,
        condition_num,
        replace_var,
        value_replace,
    )


def test_rep_variables_we(input_data_rep_variables_we):
    (
        df,
        type_of_condition,
        var_cond_rep,
        condition_num,
        replace_var,
        value_replace,
    ) = input_data_rep_variables_we

    # Call the function being tested
    new_df = _gen_rep_variables_fixedlistsimple_we(
        df,
        type_of_condition,
        var_cond_rep,
        condition_num,
        replace_var,
        value_replace,
    )

    # Test that the values were replaced as we wanted
    assert all(
        new_df.loc[new_df[var_cond_rep] > condition_num, replace_var] == value_replace,
    )

    # Test that the values were not replaced when the condition is not satisfied (checking it is not following any other elif condition)
    assert all(
        new_df.loc[new_df[var_cond_rep] <= condition_num, replace_var] != value_replace,
    )


### VARIOUS GENERATED ONES ###


def _gen_diff_diff_variables_we(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d):
    """This function is creating a variable based on a difference of already existing
    variables in a dataframe.

    It has the following inputs (new_var_d, var1_d, var2_d, factor1_d, factor2_d)

    """
    df[new_var_d] = (factor1_d * df[var1_d]) - (factor2_d * df[var2_d])
    return df


def _gen_diff_sum_variables_we(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s):
    """This function is creating a variable based on a difference of already existing
    variables in a dataframe.

    It has the following inputs (new_var_s, var1_s, var2_s, factor1_s, factor2_s)

    """
    df[new_var_s] = (factor1_s * df[var1_s]) + (factor2_s * df[var2_s])
    return df


def _gen_simple_we(df, new_var_sim, var_sim, factor_sim):
    """This function is creating a variable based on a difference of already existing
    variables in a dataframe.

    It has the following inputs (new_var_m, var1_m, var2_m, factor1_m, factor2_m)

    """
    df[new_var_sim] = df[var_sim] * factor_sim
    return df


@pytest.fixture()
def input_data_gen_diff_diff_variables_we():
    df = WeeklyPanel
    new_var_d = "jewish_int_one_block_away_1"
    var1_d = "jewish_inst_one_block_away"
    var2_d = "jewish_inst"
    factor1_d = 1
    factor2_d = 1
    return df, new_var_d, var1_d, var2_d, factor1_d, factor2_d


def input_data_gen_diff_sum_variables_we():
    df = WeeklyPanel
    new_var_s = "code2"
    var1_s = "week"
    var2_s = "n_neighborhood"
    factor1_s = 1
    factor2_s = 1000
    return df, new_var_s, var1_s, var2_s, factor1_s, factor2_s


def input_data_gen_simple_we():
    df = WeeklyPanel
    new_var_sim = "n_total_thefts"
    var_sim = "total_thefts"
    factor_sim = (365 / 12) / 7
    return df, new_var_sim, var_sim, factor_sim


def test_gen_diff_general_we(
    input_data_gen_diff_diff_variables_we,
    input_data_gen_diff_sum_variables_we,
    input_data_gen_simple_we,
):

    """First with _gen_diff_diff_variables_we."""
    (
        df,
        new_var_d,
        var1_d,
        var2_d,
        factor1_d,
        factor2_d,
    ) = input_data_gen_diff_diff_variables_we

    # Call the function being tested
    new_df1 = _gen_diff_diff_variables_we(
        df,
        df,
        new_var_d,
        var1_d,
        var2_d,
        factor1_d,
        factor2_d,
    )

    # Check that the column is now in the Dataframe
    assert new_var_d in new_df1.columns

    # Check that the value of the variable is the correct one
    assert new_df1[new_var_d] == (factor1_d * new_df1[var1_d]) - (
        factor2_d * new_df1[var2_d]
    )

    """Second with _gen_diff_sum_variables_we"""
    (
        df,
        new_var_s,
        var1_s,
        var2_s,
        factor1_s,
        factor2_s,
    ) = input_data_gen_diff_sum_variables_we

    # Call the function being tested
    new_df2 = _gen_diff_sum_variables_we(
        df,
        new_var_s,
        var1_s,
        var2_s,
        factor1_s,
        factor2_s,
    )

    # Check that the column is now in the Dataframe
    assert new_var_s in new_df2.columns

    # Check that the value of the variable is the correct one
    assert new_df2[new_var_s] == (factor1_s * new_df2[var1_s]) + (
        factor2_s * new_df2[var2_s]
    )

    """Third with input_data_gen_simple_we"""
    df, new_var_sim, var_sim, factor_sim = input_data_gen_simple_we

    # Call the function being tested
    new_df3 = _gen_simple_we(df, new_var_sim, var_sim, factor_sim)

    # Check that the column is now in the Dataframe
    assert new_var_sim in new_df3.columns

    # Check that the value of the variable is the correct one
    assert new_df3[new_var_sim] == new_df3[var_sim] * factor_sim


def _gen_rep_variables_fixedlistcomplex_we(
    df,
    list1_fix_com,
    list2_fix_com,
    var_fix_comp_mul,
    range_rep_fix_com,
    list_rep_fix_com,
    var_fix_com_to_use,
    var_fix_com_to_change,
):
    """This functions has certain inputs to generate a variable and replace its values
    (list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com,
    list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change)"""
    for col1, col2 in zip(list1_fix_com, list2_fix_com):
        df[col1] = df[col2] * df[var_fix_comp_mul]
    for col, i in zip(list_rep_fix_com, range_rep_fix_com):
        df.loc[df[var_fix_com_to_use] == col, var_fix_com_to_change] = i
    return df


@pytest.fixture()
def input_data_gen_rep_variables_fixedlistcomplex_we():
    df = WeeklyPanel
    list1_fix_com = ["jewish_inst_p", "jewish_int_one_block_away_1_p", "cuad2p"]
    list2_fix_com = ["jewish_inst", "jewish_int_one_block_away_1", "cuad2"]
    var_fix_comp_mul = "post"
    range_rep_fix_com = range(1, 4)
    list_rep_fix_com = ["Belgrano", "Once", "V. Crespo"]
    var_fix_com_to_use = "neighborhood"
    var_fix_com_to_change = "n_neighborhood"
    return (
        df,
        list1_fix_com,
        list2_fix_com,
        var_fix_comp_mul,
        range_rep_fix_com,
        list_rep_fix_com,
        var_fix_com_to_use,
        var_fix_com_to_change,
    )


def test_gen_rep_variables_fixedlistcomplex_we(
    input_data_gen_rep_variables_fixedlistcomplex_we,
):
    (
        df,
        list1_fix_com,
        list2_fix_com,
        var_fix_comp_mul,
        range_rep_fix_com,
        list_rep_fix_com,
        var_fix_com_to_use,
        var_fix_com_to_change,
    ) = input_data_gen_rep_variables_fixedlistcomplex_we

    # Call the function being tested
    new_df = _gen_rep_variables_fixedlistcomplex_we(
        df,
        list1_fix_com,
        list2_fix_com,
        var_fix_comp_mul,
        range_rep_fix_com,
        list_rep_fix_com,
        var_fix_com_to_use,
        var_fix_com_to_change,
    )

    # Test that new columns were added
    assert all(list1_fix_com) in new_df.columns

    # Test that the values were replaced correctly
    for col, i in zip(list_rep_fix_com, range_rep_fix_com):
        assert new_df.loc[new_df[var_fix_com_to_use] == col, var_fix_com_to_change] == i


MonthlyPanel, meta = pyread.read_dta(
    "/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/MonthlyPanel.dta",
)


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


@pytest.fixture()
def input_data_clean_column_names_mon():
    df = MonthlyPanel
    return df


def test__clean_column_names_mon(input_data_clean_column_names_mon):
    df = input_data_clean_column_names_mon

    # Call the function being tested
    new_df = _clean_column_names_mon(df)

    # Define list of names replaced
    list_replace = [
        "observ",
        "neighborhood",
        "street",
        "street_nr",
        "jewish_inst",
        "jewish_inst_one_block_away",
        "distance_to_jewish_inst",
        "public_building_or_embassy",
        "gas_station",
        "bank",
        "total_thefts",
        "month",
    ]

    # Assert that the new columns are there
    assert all(list_replace) in new_df.columns


### For the new test this will be used --------------
MonthlyPanel = _clean_column_names_mon(MonthlyPanel)
####-------------------------------------------------


def _gen_rep_var_fixed_extension_mon(
    df,
    range_ext,
    list_names_ext,
    original_value_var,
    range_loop,
    var_cond_ext,
    final_value_var,
):

    """This function is just generating new variables (columns) for our dataframe (df)
    given a certain condition.

    In this case, we generate variable based on a list extension (list_names_ext) which
    has a range of extension(range_ext). The extension of the list follows a condition
    of extension (ext_cond). In the end we give a value to all these variables
    (original_value_var). After that we replace the values of the variable given the
    condition that another variable (var_cond_ext) in the data frame has a certain value
    "i" which is part of a loop range (range_loop). The columns to be replaced are also
    the same as the extension condition (ext_cond) that we used to extend the list to
    generate the variables. In the end, all the columns have a fixed replaced variable
    (final_value_var)

    """

    for i in range_ext:
        list_names_ext.extend([f"month{i}"])
    df[list(list_names_ext)] = original_value_var  # generate
    for i in range_loop:
        df.loc[df[var_cond_ext] == i, f"month{i}"] = final_value_var  # replace
    return df


@pytest.fixture()
def input_data_gen_rep_var_fixed_extension_mon():
    df = MonthlyPanel
    range_ext = range(6, 13)
    list_names_ext = ["month5"]
    original_value_var = 0
    range_loop = range(5, 13)
    var_cond_ext = "month"
    final_value_var = 1
    return (
        df,
        range_ext,
        list_names_ext,
        original_value_var,
        range_loop,
        var_cond_ext,
        final_value_var,
    )


def test_gen_rep_variables_fixedextension_mon(
    input_data_gen_rep_var_fixed_extension_mon,
):
    (
        df,
        range_ext,
        list_names_ext,
        original_value_var,
        range_loop,
        var_cond_ext,
        final_value_var,
    ) = input_data_gen_rep_var_fixed_extension_mon

    # Call the function being tested
    new_df = _gen_rep_var_fixed_extension_mon(
        df,
        range_ext,
        list_names_ext,
        original_value_var,
        range_loop,
        var_cond_ext,
        final_value_var,
    )

    # Test that the new columns were added with the expected values that I assigned to them
    for i in range_ext:
        assert f"month{i}" in new_df.columns
        assert all(new_df[f"month{i}"] == original_value_var)

    # Test that the values were replaced as we wanted
    for i in range_loop:
        assert all(
            new_df.loc[new_df[var_cond_ext] == i, f"month{i}"] == final_value_var,
        )


def _gen_var_difference_mon(df, new_var, var1, var_sub):
    """This function generates a new variable (new_var) in a dataframe (df) using
    existing columns of the dataframe (var1, var_sub) and subtracting them.

    g     var_sub is the column being subtracted

    """
    df[new_var] = df[var1] - df[var_sub]
    return df


@pytest.fixture()
def input_data_gen_var_difference_mon():
    df = MonthlyPanel
    new_var = "jewish_inst_one_block_away_1"
    var1 = "jewish_inst_one_block_away"
    var_sub = "jewish_inst"
    return df, new_var, var1, var_sub


def test_gen_var_difference_mon(input_data_gen_var_difference_mon):
    df, new_var, var1, var_sub = input_data_gen_var_difference_mon

    # Call the function being tested
    new_df = _gen_var_difference_mon(df, new_var, var1, var_sub)

    # Check that the column is now in the Dataframe
    assert new_var in new_df.columns

    # Check that the value of the variable is the correct one
    assert new_df[new_var] == new_df[var1] + new_df[var_sub]


def _gen_rep_var_single_cond_biggerthan_mon(
    df,
    var_gen,
    original_value,
    cond_var,
    final_value,
    cond,
):

    """This function tries to generate a variable (var_gen) in a data frame (df) with an
    original (original_value) value to later on replace it given a condition of another
    column (cond_var) given it a specific value (final_value) if the bigger than
    condition (cond) is met."""

    df[var_gen] = original_value
    df.loc[df[cond_var] > cond, var_gen] = final_value
    return df


@pytest.fixture()
def input_data_gen_rep_var_single_cond_biggerthan_mon():
    df = MonthlyPanel
    var_gen = "post"
    original_value = 0
    cond_var = "month"
    final_value = 1
    cond = 7
    return df, var_gen, original_value, cond_var, final_value, cond


def test_gen_rep_var_single_cond_biggerthan_mon(
    input_data_gen_rep_var_single_cond_biggerthan_mon,
):
    (
        df,
        var_gen,
        original_value,
        cond_var,
        final_value,
        cond,
    ) = input_data_gen_rep_var_single_cond_biggerthan_mon

    # Call the function being tested
    new_df = _gen_rep_var_single_cond_biggerthan_mon(
        df,
        var_gen,
        original_value,
        cond_var,
        final_value,
        cond,
    )

    # Test that the values were replaced as we wanted
    assert all(new_df.loc[new_df[cond_var] > cond, var_gen] == final_value)


def _gen_rep_var_fixed_extension_mon2(
    df,
    range_ext2,
    list_names_ext2,
    original_value_var2,
    range_loop2,
    var_cond_ext2,
    final_value_var2,
):

    """This function is just generating new variables (columns) for our dataframe (df)
    given a certain condition.

    In this case, we generate variable based on a list extension (list_names_ext) which
    has a range of extension(range_ext). The extension of the list follows a condition
    of extension (ext_cond). In the end we give a value to all these variables
    (original_value_var). After that we replace the values of the variable given the
    condition that another variable (var_cond_ext) in the data frame has a certain value
    "i" which is part of a loop range (range_loop). The columns to be replaced are also
    the same as the extension condition (ext_cond) that we used to extend the list to
    generate the variables. In the end, all the columns have a fixed replaced variable
    (final_value_var)

    """

    for i in range_ext2:
        list_names_ext2.extend([f"cuad{i}"])
    df[list(list_names_ext2)] = original_value_var2  # generate
    for i in range_loop2:
        df.loc[df[var_cond_ext2] == i, f"cuad{i}"] = final_value_var2  # replace
    return df


@pytest.fixture()
def input_data_gen_rep_var_fixed_extension_mon2():
    df = MonthlyPanel
    range_ext2 = range(1, 8)
    list_names_ext2 = ["cuad0"]
    original_value_var2 = 0
    range_loop2 = range(0, 8)
    var_cond_ext2 = "distance_to_jewish_inst"
    final_value_var2 = 1
    return (
        df,
        range_ext2,
        list_names_ext2,
        original_value_var2,
        range_loop2,
        var_cond_ext2,
        final_value_var2,
    )


def test_gen_rep_variables_fixedextension_mon(
    input_data_gen_rep_var_fixed_extension_mon2,
):
    (
        df,
        range_ext2,
        list_names_ext2,
        original_value_var2,
        range_loop2,
        var_cond_ext2,
        final_value_var2,
    ) = input_data_gen_rep_var_fixed_extension_mon2

    # Call the function being tested
    new_df = _gen_rep_var_fixed_extension_mon2(
        df,
        range_ext2,
        list_names_ext2,
        original_value_var2,
        range_loop2,
        var_cond_ext2,
        final_value_var2,
    )

    # Test that the new columns were added with the expected values that I assigned to them
    for i in range_ext2:
        assert f"cuad{i}" in new_df.columns
        assert all(new_df[f"cuad{i}"] == original_value_var2)

    # Test that the values were replaced as we wanted
    for i in range_loop2:
        assert all(
            new_df.loc[new_df[var_cond_ext2] == i, f"cuad{i}"] == final_value_var2,
        )


# from clean_data import _gen_rep_var_fixed_extension_mon2]


def _gen_var_cond_list_similar_mon(df, ori_variables, fixed_variable, name_change):
    """"This functions is trying to generate a list of variables that have a quite
    similar name compared to an already existing set of variables (ri_variables) in a
    dataframe (df) and it is generated by giving them a similar name with a new added
    condition to the original ones (name_change) and by multiplying the original columns
    of the df on a loop by a fixed column (fixed_variable) in the dataframe."""

    for col in ori_variables:
        df[col + name_change] = df[col] * df[fixed_variable]
    return df


@pytest.fixture()
def input_data_gen_var_cond_list_similar_mon():
    df = MonthlyPanel
    ori_variables = [
        "cuad0",
        "cuad1",
        "cuad2",
        "cuad3",
        "cuad4",
        "cuad5",
        "cuad6",
        "cuad7",
    ]
    fixed_variable = "post"
    name_change = "p"
    return df, ori_variables, fixed_variable, name_change


def test_gen_var_cond_list_similar_mon(input_data_gen_var_cond_list_similar_mon):
    (
        df,
        ori_variables,
        fixed_variable,
        name_change,
    ) = input_data_gen_var_cond_list_similar_mon

    # Call the function being tested
    new_df = _gen_var_cond_list_similar_mon(
        df,
        ori_variables,
        fixed_variable,
        name_change,
    )

    # Test that new columns were added
    list_var = [
        "cuad0p",
        "cuad1p",
        "cuad2p",
        "cuad3p",
        "cuad4p",
        "cuad5p",
        "cuad6p",
        "cuad7p",
    ]
    assert all(list_var) in new_df.columns


# from clean_data _gen_rep_var_various_cond_equality_mon


def _gen_rep_var_various_cond_equality_mon(
    df,
    new_gen_variable,
    new_original_value,
    list_ext_variables,
    range_new_gen,
    value_originallist,
):
    """This function is generating a new variable (new_gen_variable) in a dataframe (df)
    with an original value (new_original_value).

    The values are replace with different values (range_new_gen) on a loop for the
    original list of variables (list_ext_variables, range_new_gen) depending on
    different conditions on other variables (value_originallist) already existing in the
    data frame

    """
    df[new_gen_variable] = new_original_value  # generate
    for col, i in zip(list_ext_variables, range_new_gen):
        df.loc[df[col] == value_originallist, new_gen_variable] = i  # replace
    return df


@pytest.fixture()
def input_data_gen_rep_var_various_cond_equality_mon():
    df = MonthlyPanel
    new_gen_variable = "code"
    new_original_value = 4
    list_ext_variables = ["jewish_inst", "jewish_inst_one_block_away_1", "cuad2"]
    range_new_gen = range(1, 4)
    value_originallist = 1
    return (
        df,
        new_gen_variable,
        new_original_value,
        list_ext_variables,
        range_new_gen,
        value_originallist,
    )


def test_gen_rep_var_various_cond_equality_mon(
    input_data_gen_rep_var_various_cond_equality_mon,
):
    (
        df,
        new_gen_variable,
        new_original_value,
        list_ext_variables,
        range_new_gen,
        value_originallist,
    ) = input_data_gen_rep_var_various_cond_equality_mon

    # Call the function being tested
    new_df = _gen_rep_var_various_cond_equality_mon(
        df,
        new_gen_variable,
        new_original_value,
        list_ext_variables,
        range_new_gen,
        value_originallist,
    )

    # Test that new columns were added
    assert new_gen_variable in new_df.columns

    # Test that the values were replaced correctly
    for col, i in zip(list_ext_variables, range_new_gen):
        assert new_df.loc[new_df[col] == value_originallist, new_gen_variable] == i


def _gen_var_double_listed_mon(df, list_gen_var, list_ori_var, fixed_var):
    """This function is generating variables listed on a list (list_gen_var) given a
    list of existing variables (list_ori_var) within a dataframe (df) using a
    multiplication rule multiplying it by a fixed variable (fixed_var) already existent
    in thedataframe(df)"""
    for col1, col2 in zip(list_gen_var, list_ori_var):
        df[col1] = df[col2] * df[fixed_var]
    return df


@pytest.fixture()
def input_data_gen_var_double_listed_mon():
    df = MonthlyPanel
    list_gen_var = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
    list_ori_var = ["jewish_inst", "jewish_inst_one_block_away_1"]
    fixed_var = "post"
    return df, list_gen_var, list_ori_var, fixed_var


def test_gen_var_double_listed_mon(input_data_gen_var_double_listed_mon):
    df, list_gen_var, list_ori_var, fixed_var = input_data_gen_var_double_listed_mon

    # Call the function being tested
    new_df = _gen_var_double_listed_mon(df, list_gen_var, list_ori_var, fixed_var)

    # Test that new columns were added
    assert all(list_gen_var) in new_df.columns

    # Test that the values were replaced correctly
    for col1, col2 in zip(list_gen_var, list_ori_var):
        assert new_df[col1] == new_df[col2] * df[fixed_var]


def _gen_rep_var_various_cond_equality_listedvalues_mon(
    df,
    NEW_var,
    ORI_var,
    list_a,
    list_b,
):
    """This function is generating a new variable (NEW_var) with an original value
    (ORI_var) and then replacing it with given a condition on an existent variable
    (list_a) on a dataframe and using elements of a list to replace the value
    (list_b)"""
    df[NEW_var] = df[ORI_var]
    for i, j in zip(list_a, list_b):
        df.loc[df[ORI_var] == i, NEW_var] = j
    return df


@pytest.fixture()
def input_data_gen_rep_var_various_cond_equality_listedvalues_mon():
    df = MonthlyPanel
    NEW_var = "othermonth1"
    ORI_var = "month"
    list_a = [72, 73]
    list_b = [7.2, 7.3]
    return df, NEW_var, ORI_var, list_a, list_b


def test_gen_rep_var_various_cond_equality_listedvalues_mon(
    input_data_gen_rep_var_various_cond_equality_listedvalues_mon,
):
    (
        df,
        NEW_var,
        ORI_var,
        list_a,
        list_b,
    ) = input_data_gen_rep_var_various_cond_equality_listedvalues_mon

    # Call the function being tested
    new_df = _gen_rep_var_various_cond_equality_listedvalues_mon(
        df,
        NEW_var,
        ORI_var,
        list_a,
        list_b,
    )

    # Test that new columns were added
    assert all(new_df[NEW_var]) == all(new_df[ORI_var])

    # Test that the values were replaced correctly
    for i, j in zip(list_a, list_b):
        assert new_df.loc[new_df[ORI_var] == i, NEW_var] == j


def _sort_mon(df, list_sort):
    """This functions is sorting the values of a dataframe (df) given a list of values
    to be used for sorting (list_sort)"""
    df = df.sort_values(list_sort)
    return df


@pytest.fixture()
def input_data_sort_mon():
    # Create a sample dataframe
    df = pd.DataFrame(
        {
            "observ": [1, 2, 3],
            "month": ["January", "February", "March"],
            "value1": [10, 5, 20],
            "value2": [30, 15, 5],
        },
    )  # using a reference DataFrame
    list_sort = ["observ", "month"]
    return df, list_sort


def test_sort_mon(input_data_sort_mon):
    # Unpack the input data
    df, list_sort = input_data_sort_mon

    # Call the function
    df_output = _sort_mon(df, list_sort)

    # Check that the output dataframe is sorted correctly
    assert df_output.iloc[0]["month"] == "January"
    assert df_output.iloc[-1]["month"] == "March"
    assert df_output.iloc[0]["observ"] == 1
    assert df_output.iloc[-1]["observ"] == 3


def _gen_rep_total_thefts2_mon(df, var_complex_cond, cond1, cond2):
    """This function is generating a a new variable by observation "total_thefts2" in a
    dataframe (df)"""
    df = df.assign(total_thefts2=pd.Series())
    df["total_thefts2"] = df["total_thefts2"].tolist()
    for i in range(1, len(df)):
        if (
            df[var_complex_cond].iloc[i] == cond1
            or df[var_complex_cond].iloc[i] == cond2
        ):
            df["total_thefts2"].loc[i] == df["total_thefts"].iloc[
                i
            ].cumsum()  # generate
    df.loc[
        (df[var_complex_cond] != cond1) & (df[var_complex_cond] != cond2),
        "total_thefts2",
    ] = df[
        "total_thefts"
    ]  # replace
    df["total_thefts2"] = pd.Series(df["total_thefts2"])
    return df


@pytest.fixture()
def input_data_gen_rep_total_thefts2_mon():
    df = pd.DataFrame(
        {
            "month": [72, 73, 74, 75, 76, 77],
            "total_thefts": [10, 20, 30, 40, 50, 60],
        },
    )  # using a reference DataFrame because the function is more complex to grasp
    var_complex_cond = "month"
    cond1 = 72
    cond2 = 73
    return df, var_complex_cond, cond1, cond2


def test_gen_rep_total_thefts2_mon(input_data_gen_rep_total_thefts2_mon):
    df, var_complex_cond, cond1, cond2 = input_data_gen_rep_total_thefts2_mon

    # Call the function being tested
    new_df = _gen_rep_total_thefts2_mon(df, var_complex_cond, cond1, cond2)

    # Test that new columns were added
    expected_output = pd.DataFrame(
        {
            "month": [
                72,
                73,
                74,
                75,
                76,
                77,
            ],  # if month = 72 o 73 the total_thefs2 is equal to the cumulative sum
            "total_thefts": [10, 20, 30, 40, 50, 60],
            "total_thefts2": [10, 30, 30, 40, 50, 60],  # cumulative sum
        },
    )
    pd.testing.assert_frame_equal(new_df, expected_output)
