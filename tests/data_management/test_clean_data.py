import numpy as numpy
import pandas as pd
import pyreadstat  as pyread
import pytest

"""""
from di_tella_2004_replication.config import TEST_DIR
from di_tella_2004_replication.data_management import clean_data
from di_tella_2004_replication.utilities import read_yaml

"""""
"""""

@pytest.fixture()
def data():
    return pd.read_csv(TEST_DIR / "data_management" / "data_fixture.csv")


@pytest.fixture()
def data_info():
    return read_yaml(TEST_DIR / "data_management" / "data_info_fixture.yaml")


def test_clean_data_drop_columns(data, data_info):
    data_clean = clean_data(data, data_info)
    assert not set(data_info["columns_to_drop"]).intersection(set(data_clean.columns))


def test_clean_data_dropna(data, data_info):
    data_clean = clean_data(data, data_info)
    assert not data_clean.isna().any(axis=None)


def test_clean_data_categorical_columns(data, data_info):
    data_clean = clean_data(data, data_info)
    for cat_col in data_info["categorical_columns"]:
        cat_col = data_info["column_rename_mapping"].get(cat_col, cat_col)
        assert data_clean[cat_col].dtype == "category"


def test_clean_data_column_rename(data, data_info):
    data_clean = clean_data(data, data_info)
    old_names = set(data_info["column_rename_mapping"].keys())
    new_names = set(data_info["column_rename_mapping"].values())
    assert not old_names.intersection(set(data_clean.columns))
    assert new_names.intersection(set(data_clean.columns)) == new_names


def test_convert_outcome_to_numerical(data, data_info):
    data_clean = clean_data(data, data_info)
    outcome_name = data_info["outcome"]
    outcome_numerical_name = data_info["outcome_numerical"]
    assert outcome_numerical_name in data_clean.columns
    assert data_clean[outcome_name].dtype == "category"
    assert data_clean[outcome_numerical_name].dtype == np.int8

"""""

"""Test WEEKLY DATA"""

# from clean_data import _gen_rep_variables_fixedextension_we

WeeklyData = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/WeeklyPanel.dta')

def _gen_rep_variables_fixedextension_we(df,list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext):    
    """"This functions has certain inputs to generate a variable and replace its values (list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext)""" 
    for i in range_ext:
        list_names_ext.extend([f"week{i}"])  
    df[[col for col in list_names_ext]] = original_value_var # generate
    for i in range_loop:
        df.loc[df[var_cond_ext]==i, f"week{i}"]= final_value_var # replace
    return df

@pytest.fixture
def input_data_gen_rep_variables_fixedextension_we():
    df = WeeklyData
    list_names_ext = ["week1"]
    range_ext = range(2,40)
    original_value_var = 0
    final_value_var = 0
    range_loop = range(1, 40)
    var_cond_ext = 'week'
    return df, list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext


def test_gen_rep_variables_fixedextension_we(input_data_gen_rep_variables_fixedextension_we):  
    df, list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext = input_data_gen_rep_variables_fixedextension_we

    # Call the function being tested
    new_df = _gen_rep_variables_fixedextension_we(df, list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext)

    # Test that the new columns were added with the expected values that I assigned to them
    for i in range_ext:
        assert f"week{i}" in new_df.columns
        assert all(new_df[f"week{i}"] == original_value_var)

    # Test that the values were replaced as we wanted
    for i in range_loop:
        assert all(new_df.loc[new_df[var_cond_ext] == i, f"week{i}"] == final_value_var)
        



# from clean_data import _gen_rep_variables_fixedlistsimple_we  
      
def _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix): 
    """This functions has certain inputs to generate a variable and replace its values (list_fixed, var_cond_fix, var_fix, value_var_fix)"""
    df[[col for col in list_fixed]] = 0 # generate
    df.loc[df[var_cond_fix]==cond_fix, var_fix]= value_var_fix # replace
    return df

@pytest.fixture
def input_data_gen_rep_variables_fixedlistsimple_we():
    df = WeeklyData
    list_fixed = ["cuad2", "post", "n_neighborhood"]
    var_cond_fix = 'distance_to_jewish_inst'
    cond_fix = 2
    var_fix = 'cuad2'
    value_var_fix = 1
    return df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix

def test_gen_rep_variables_fixedlistsimple_we(input_data_gen_rep_variables_fixedlistsimple_we):
    df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix = input_data_gen_rep_variables_fixedlistsimple_we

    # Call the function being tested
    new_df = _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix)

    # Test that the new columns were added 
    assert all(list_fixed) in new_df.columns
    
    # Test that the values were replaced as we wanted
    assert all(new_df.loc[new_df[var_cond_fix] == cond_fix, list_fixed] == value_var_fix)
    


# from clean_data import _rep_variables_we

def _rep_variables_we(df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace):
    
    """What this function does is just to replace a variable from a data frame depending on different types of conditions and with inputs 
    (var_cond_rep, condition_num, replace_var, value_replace)"""
    
    if type_of_condition == "bigger than":
        df.loc[df[var_cond_rep]>condition_num, replace_var]=value_replace
        return df
    elif type_of_condition == "smaller than":
        df.loc[df[var_cond_rep]<condition_num, replace_var]=value_replace
        return df
    elif type_of_condition == "equal to":
        df.loc[df[var_cond_rep]>condition_num, replace_var]=value_replace
        return df
    
@pytest.fixture
def input_data_rep_variables_we():
    df = WeeklyData
    type_of_condition = "bigger than"
    var_cond_rep = 'week'
    condition_num = 18
    replace_var = 'post'
    value_replace = 1
    return df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace

def test_rep_variables_we(input_data_rep_variables_we):   
    df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace = input_data_rep_variables_we

    # Call the function being tested
    new_df = _gen_rep_variables_fixedlistsimple_we(df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace)
    
    # Test that the values were replaced as we wanted
    assert all(new_df.loc[new_df[var_cond_rep] > condition_num, replace_var] == value_replace)
    
    # Test that the values were not replaced when the condition is not satisfied (checking it is not following any other elif condition)
    assert all(new_df.loc[new_df[var_cond_rep] <= condition_num, replace_var] != value_replace)


### VARIOUS GENERATED ONES ###

# from clean_data import _gen_diff_diff_variables_we
def _gen_diff_diff_variables_we(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_d, var1_d, var2_d, factor1_d, factor2_d)"""
    df[new_var_d] = (factor1_d*df[var1_d]) - (factor2_d*df[var2_d])
    return df

# from clean_data import _gen_diff_sum_variables_we
def _gen_diff_sum_variables_we(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_s, var1_s, var2_s, factor1_s, factor2_s)"""
    df[new_var_s] = (factor1_s*df[var1_s]) + (factor2_s*df[var2_s])
    return df

# from clean_data import _gen_simple_we 
def _gen_simple_we(df, new_var_sim, var_sim, factor_sim):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_m, var1_m, var2_m, factor1_m, factor2_m)"""
    df[new_var_sim] = df[var_sim]*factor_sim
    return df


@pytest.fixture
def input_data_gen_diff_diff_variables_we():
    df = WeeklyData
    new_var_d = 'jewish_int_one_block_away_1'
    var1_d = 'jewish_inst_one_block_away'
    var2_d = 'jewish_inst'
    factor1_d = 1
    factor2_d = 1
    return df, new_var_d, var1_d, var2_d, factor1_d, factor2_d
def input_data_gen_diff_sum_variables_we():
    df = WeeklyData
    new_var_s = 'code2'
    var1_s = 'week'
    var2_s = 'n_neighborhood'
    factor1_s = 1
    factor2_s = 1000
    return df, new_var_s, var1_s, var2_s, factor1_s, factor2_s
def input_data_gen_simple_we():
    df = WeeklyData
    new_var_sim = 'n_total_thefts'
    var_sim = 'total_thefts'
    factor_sim = (365/12)/7 
    return df, new_var_sim, var_sim, factor_sim


def test_gen_diff_general_we(input_data_gen_diff_diff_variables_we, input_data_gen_diff_sum_variables_we, input_data_gen_simple_we):
    
    """First with _gen_diff_diff_variables_we"""
    df, new_var_d, var1_d, var2_d, factor1_d, factor2_d = input_data_gen_diff_diff_variables_we

    # Call the function being tested
    new_df1 = _gen_diff_diff_variables_we(df, df, new_var_d, var1_d, var2_d, factor1_d, factor2_d)
    
    # Check that the column is now in the Dataframe
    assert new_var_d in new_df1.columns
    
    # Check that the value of the variable is the correct one
    assert new_df1[new_var_d] == (factor1_d*new_df1[var1_d]) - (factor2_d*new_df1[var2_d])
    
    """Second with _gen_diff_sum_variables_we"""
    df, new_var_s, var1_s, var2_s, factor1_s, factor2_s = input_data_gen_diff_sum_variables_we
    
    # Call the function being tested
    new_df2 = _gen_diff_sum_variables_we(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s)
    
    # Check that the column is now in the Dataframe
    assert new_var_s in new_df2.columns
    
    # Check that the value of the variable is the correct one
    assert new_df2[new_var_s] == (factor1_s*new_df2[var1_s]) + (factor2_s*new_df2[var2_s])
    
    """Third with input_data_gen_simple_we"""
    df, new_var_sim, var_sim, factor_sim = input_data_gen_simple_we
    
    # Call the function being tested
    new_df3 = _gen_simple_we(df, new_var_sim, var_sim, factor_sim)
    
    # Check that the column is now in the Dataframe
    assert new_var_sim in new_df3.columns
    
    # Check that the value of the variable is the correct one
    assert new_df3[new_var_sim] == new_df3[var_sim]*factor_sim
    
    


# from clean_data import _gen_rep_variables_fixedlistcomplex_we

def _gen_rep_variables_fixedlistcomplex_we(df, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change): 
    """This functions has certain inputs to generate a variable and replace its values 
    (list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change)"""
    for col1, col2 in zip(list1_fix_com, list2_fix_com):
        df[col1] = df[col2]*df[var_fix_comp_mul]
    for col, i in zip(list_rep_fix_com, range_rep_fix_com):
        df.loc[df[var_fix_com_to_use]==col, var_fix_com_to_change]=i
    return df

@pytest.fixture
def input_data_gen_rep_variables_fixedlistcomplex_we():
    df = WeeklyData
    list1_fix_com = ["jewish_inst_p", "jewish_int_one_block_away_1_p", "cuad2p"]
    list2_fix_com = ["jewish_inst",  "jewish_int_one_block_away_1", "cuad2"]
    var_fix_comp_mul = 'post'
    range_rep_fix_com = range(1,4)
    list_rep_fix_com = ["Belgrano", "Once", "V. Crespo"]
    var_fix_com_to_use = 'neighborhood'
    var_fix_com_to_change = 'n_neighborhood'
    return df, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change

def test_gen_rep_variables_fixedlistcomplex_we(input_data_gen_rep_variables_fixedlistcomplex_we):
    df, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change = input_data_gen_rep_variables_fixedlistcomplex_we
    
    # Call the function being tested
    new_df = _gen_rep_variables_fixedlistcomplex_we(df, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change)
    
    # Test that new columns were added
    assert all(list1_fix_com) in new_df.columns
    
    # Test that the values were replaced correctly
    for col, i in zip(list_rep_fix_com, range_rep_fix_com):
        assert new_df.loc[new_df[var_fix_com_to_use]==col, var_fix_com_to_change]==i


    
    
    
    
    
    

# 