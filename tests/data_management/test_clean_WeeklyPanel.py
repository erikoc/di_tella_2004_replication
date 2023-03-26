import pandas as pd
import pyreadstat  as pyread
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_WeeklyPanel import (
    _clean_column_names_we, _gen_rep_variables_fixedextension_we, _gen_rep_variables_fixedlistsimple_we, _rep_variables_we, _gen_rep_variables_fixedlistcomplex_we
)


"Fixtures"

@pytest.fixture()
def original_data():
    return {
        "weekly_panel": pyread.read_dta(SRC / "data" / "WeeklyPanel.dta"), 
    }


### For the new test this will be used --------------
WeeklyPanel = _clean_column_names_we(original_data["weekly_panel"]) # for the test we need to use the new dataframe with the new column names
####-------------------------------------------------
    
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

@pytest.fixture()
def input_data_gen_rep_variables_fixedlistsimple_we():
    df = WeeklyPanel
    list_fixed = ["cuad2", "post", "n_neighborhood"]
    var_cond_fix = "distance_to_jewish_inst"
    cond_fix = 2
    var_fix = "cuad2"
    value_var_fix = 1
    return df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix


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
    
    




"Tests"


def test_clean_column_names_we(original_data):
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