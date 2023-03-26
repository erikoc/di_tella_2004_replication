import pandas as pd
import pyreadstat  as pyread
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_MonthlyPanel import (
    _clean_column_names_mon, _gen_rep_var_fixed_extension_mon, _gen_rep_var_single_cond_biggerthan_mon, _gen_var_cond_list_similar_mon, _gen_rep_var_various_cond_equality_mon, _gen_var_double_listed_mon, _gen_rep_var_various_cond_equality_listedvalues_mon, _gen_rep_total_thefts2_mon,
    _gen_rep_various_cond_mon2, _genNA_rep_two_cond_mon2
)


"Fixtures"

############################################## PART 1 ########################################################################################################################

@pytest.fixture()
def original_data():
    return {
        "monthly_panel": pyread.read_dta(SRC / "data" / "MonthlyPanel.dta"),
    }

### For the new test this will be used --------------
MonthlyPanel = _clean_column_names_mon(original_data["monthly_panel"])
####-------------------------------------------------

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
    
    
@pytest.fixture()
def input_data_gen_rep_var_single_cond_biggerthan_mon():
    df = MonthlyPanel
    var_gen = "post"
    original_value = 0
    cond_var = "month"
    final_value = 1
    cond = 7
    return (
        df, 
        var_gen, 
        original_value, 
        cond_var, 
        final_value, cond
    )
    

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
    return (
        df, 
        ori_variables, 
        fixed_variable, 
        name_change
    )
    
    
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

  
@pytest.fixture()
def input_data_gen_var_double_listed_mon():
    df = MonthlyPanel
    list_gen_var = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
    list_ori_var = ["jewish_inst", "jewish_inst_one_block_away_1"]
    fixed_var = "post"
    return (
        df, 
        list_gen_var, 
        list_ori_var, 
        fixed_var
    )
 
@pytest.fixture()
def input_data_gen_rep_var_various_cond_equality_listedvalues_mon():
    df = MonthlyPanel
    NEW_var = "othermonth1"
    ORI_var = "month"
    list_a = [72, 73]
    list_b = [7.2, 7.3]
    return (
        df, 
        NEW_var, 
        ORI_var, 
        list_a, 
        list_b
    )


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
    return (
        df, 
        var_complex_cond, 
        cond1, 
        cond2
    )

############################################## PART 2 ########################################################################################################################
### For the new test this will be used --------------
MonthlyPanel2 = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel.csv')
####-------------------------------------------------

@pytest.fixture()
def input_data_gen_rep_various_cond_mon2():
    df = pd.DataFrame(
        {
            "var_con_v_cond": [7, 5, 8, 10, 12],
            "ori_var_v_cond": [1, 2, 3, 4, 5],
        }
    )
    new_var_v_cond = "new_var"
    con_v_cond1 = 7
    con_v_cond2 = 5
    con_v_cond3 = 8
    con_v_cond4 = 10
    con_v_cond5 = 12
    multiple1_v_cond = 30/17
    multiple2_v_cond = 30/31
    return (
        df, 
        new_var_v_cond, 
        "ori_var_v_cond", 
        "var_con_v_cond", 
        con_v_cond1, 
        con_v_cond2, 
        con_v_cond3, 
        con_v_cond4, 
        con_v_cond5, 
        multiple1_v_cond, 
        multiple2_v_cond
    )
 
@pytest.fixture()
def input_data_genNA_rep_two_cond_mon2():
    df = pd.DataFrame(
        {
            "month": [72, 73, 74, 75, 76, 77],
            "theft1": [10, 20, 30, 40, 50, 60],
            "theft2": [11, 21, 31, 41, 51, 61],
            "theft3": [12, 22, 32, 42, 52, 62],
        }
    )  # using a reference DataFrame because the function is more complex to grasp
    list_for_NA = ["theft2_NA", "theft3_NA"]
    var_con_NA = "month"
    fixed_var_NA = "theft1"
    NA_value = 73
    return (
        df,
        list_for_NA,
        var_con_NA,
        fixed_var_NA,
        NA_value,
    )
    
"Tests"

############################################## PART 1 ########################################################################################################################

def test_clean_column_names_mon(original_data):
    # Call the function being tested
    new_df = _clean_column_names_mon(original_data["monthly_panel"])

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
        


def test_gen_var_double_listed_mon(input_data_gen_var_double_listed_mon
):
    (
     df, 
     list_gen_var, 
     list_ori_var, 
     fixed_var) = input_data_gen_var_double_listed_mon

    # Call the function being tested
    new_df = _gen_var_double_listed_mon(
        df, 
        list_gen_var, 
        list_ori_var, 
        fixed_var
    )

    # Test that new columns were added
    assert all(list_gen_var) in new_df.columns

    # Test that the values were replaced correctly
    for col1, col2 in zip(list_gen_var, list_ori_var):
        assert new_df[col1] == new_df[col2] * df[fixed_var]



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

############################################## PART 2 ########################################################################################################################

def test_gen_rep_various_cond_mon2(input_data_gen_rep_various_cond_mon2):
    df, new_var_v_cond, ori_var_v_cond, var_con_v_cond, con_v_cond1, con_v_cond2, con_v_cond3, con_v_cond4, con_v_cond5, multiple1_v_cond, multiple2_v_cond = input_data_gen_rep_various_cond_mon2

    # Call the function being tested
    new_df = _gen_rep_various_cond_mon2(df, new_var_v_cond, ori_var_v_cond, var_con_v_cond, con_v_cond1, con_v_cond2, con_v_cond3, con_v_cond4, con_v_cond5, multiple1_v_cond, multiple2_v_cond)

    # Test that new columns were added
    expected_output = pd.DataFrame(
        {
            "var_con_v_cond": [7, 5, 8, 10, 12],
            "ori_var_v_cond": [1, 2, 3, 4, 5],
            "new_var": [30/17, 60/31, 90/31, 120/31, 150/31],
        }
    )
    pd.testing.assert_frame_equal(new_df, expected_output)


    
def test_genNA_rep_two_cond_mon2(input_data_genNA_rep_two_cond_mon2):
    df, list_for_NA, var_con_NA, fixed_var_NA, NA_value = input_data_genNA_rep_two_cond_mon2

    # Call the function being tested
    new_df = _genNA_rep_two_cond_mon2(df, list_for_NA, var_con_NA, fixed_var_NA, NA_value)

    # Test that new columns were added
    expected_output = pd.DataFrame({
            "month": [1, 2, 3, 4, 5, 6],
            "var1": [5, 10, 3, 4, 9, 2],
            "var2": [20, 5, 15, 8, 10, 12],
            "var1_NA": [pd.NA, pd.NA, 3, 4, pd.NA, 2],
            "var2_NA": [20, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
        })
    pd.testing.assert_frame_equal(new_df, expected_output)