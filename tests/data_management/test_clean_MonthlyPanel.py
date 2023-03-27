import pandas as pd
import numpy as np
import pyreadstat  as pyread
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_MonthlyPanel import (
    _clean_column_names_mon, _gen_rep_var_fixed_extension_mon, _gen_rep_var_single_cond_biggerthan_mon, _gen_var_cond_list_similar_mon, _gen_rep_var_various_cond_equality_mon, _gen_var_double_listed_mon, _gen_rep_var_various_cond_equality_listedvalues_mon, _gen_rep_total_thefts2_mon,
    _gen_rep_various_cond_mon2, _egen_rep_mon2, _complex_gen_rep_mon2, _gen_rep_simple_mon2, _gen_based_variouslists_mon2,
    _gen_rep_3cond_mon3, _gen_multiplevariables_listbased
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

@pytest.fixture() # Test using our own data set
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
    
    
@pytest.fixture() # Test using our own data set
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
    

@pytest.fixture() # Test using our own data set
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
    
    
@pytest.fixture() # Test using our own data set
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

  
@pytest.fixture() # Test using our own data set
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
 
@pytest.fixture() # Test using our own data set
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

@pytest.fixture
def input_data_gen_rep_various_cond_mon2():
    df = pd.DataFrame({
        'month': [1, 2, 3, 4, 5],
        'total_thefts': [10, 20, 30, 40, 50]
    })
    return df


@pytest.fixture
def input_data_egen_rep_mon2():
    df = pd.DataFrame({'observ': [1, 1, 1, 1, 1], 
                       'month': [1, 2, 3, 4, 5],
                       'prethefts': [1, 2, 3, 4, 5],
                       'theftscoll': [1, 2, 3, 4, 5]})
    return df


@pytest.fixture
def input_data_complex_gen_rep_mon2():
    df = pd.DataFrame({'month': [1, 2, 3, 4, 5],
            'neighborhood': ['Belgrano', 'Once', 'V. Crespo', 'Belgrano', 'Once'],
            'n_neighborhood': [0, 0, 0, 0, 0]})
    return df


@pytest.fixture()
def input_data_gen_rep_simple_mon2(): # Test using our own data set
    df = MonthlyPanel2
    var_gen_simple = 'month4'
    var_cond_simple = 'month'
    original_val_simple=0
    cond_simple=4 
    value_final_simple=1
    return (
        df, 
        var_gen_simple, 
        var_cond_simple,
        original_val_simple,
        cond_simple,
        value_final_simple
    )


@pytest.fixture()
def input_data_gen_based_variouslists_mon2(): # Test using our own data set
    df = MonthlyPanel2
    list_names_place = ["mbelgapr"]
    list_names_month = ["month4"]      
    list_names_variouslists = ['belgrano', 'once', 'vcrespo']
    return (
        df, 
        list_names_place, 
        list_names_month,
        list_names_variouslists
    )

############################################## PART 3 ########################################################################################################################
### For the new test this will be used --------------
MonthlyPanel3 = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel2.csv')
####-------------------------------------------------

@pytest.fixture
def input_data_gen_rep_3cond_mon3():
    df= pd.DataFrame({'public_building_or_embassy': [0, 1, 0, 0],
            'gas_station': [1, 0, 0, 1],
            'bank': [0, 1, 0, 0]})
    initial_val_3cond = 0
    global_replace_val_3cond = 1
    gen_var_3cond = 'all_locations'
    col1_3cond = 'public_building_or_embassy'
    col2_3cond = 'gas_station'
    col3_3cond = 'bank'
        
    return (
        df,
        initial_val_3cond,
        global_replace_val_3cond,
        gen_var_3cond,
        col1_3cond,
        col2_3cond,
        col3_3cond
    )
    

@pytest.fixture
def input_data_gen_multiplevariables_listbased():  # Test using our own data set
    df= MonthlyPanel3
    list_value1 = ['public_building_or_embassy_p', 'public_building_or_embassy_1_p', 'public_building_or_embassy_cuad2p', 'n_public_building_or_embassy_p', 'n_public_building_or_embassy_1_p', 'n_public_building_or_embassy_cuad2p']
    list_value2 = ['gas_station_p', 'gas_station_1_p', 'gas_station_cuad2p', 'n_gas_station_p', 'n_gas_station_1_p', 'n_gas_station_cuad2p']
    list_value3 = ['bank_p', 'bank_1_p', 'bank_cuad2p', 'n_bank_p', 'n_bank_1_p', 'n_bank_cuad2p']
    list_value4 = ['all_locations_p', 'all_locations_1_p', 'all_locations_cuad2p', 'n_all_locations_p', 'n_all_locations_1_p', 'n_all_locations_cuad2p']
    list_values = [list_value1, list_value2, list_value3, list_value4]
    list_names_3_variables = ['public_building_or_embassy', 'gas_station', 'bank', 'all_locations']
    list_names_data3_general = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p'] 
    return (
        df,
        list_values,
        list_names_3_variables,
        list_names_data3_general,
    )








"Tests"

############################################## PART 1 ########################################################################################################################

def test_clean_column_names_mon(original_data): # Test using our own data set
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
    
 
    
def test_gen_rep_variables_fixedextension_mon( # Test using our own data set
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
        

        
def test_gen_rep_var_single_cond_biggerthan_mon( # Test using our own data set
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



def test_gen_var_cond_list_similar_mon(input_data_gen_var_cond_list_similar_mon): # Test using our own data set
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



def test_gen_rep_var_various_cond_equality_mon( # Test using our own data set
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
        


def test_gen_var_double_listed_mon(input_data_gen_var_double_listed_mon # Test using our own data set
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



def test_gen_rep_var_various_cond_equality_listedvalues_mon( # Test using our own data set
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
    df = _gen_rep_various_cond_mon2(
        input_data_gen_rep_various_cond_mon2, 
        new_var_v_cond='total_thefts_c', 
        ori_var_v_cond='total_thefts', 
        var_con_v_cond='month'
    )
    assert list(df['total_thefts_c']) == [10, 20, 30, 40, 50] # assert that in the beginning it takes the values of the original variables

    df = _gen_rep_various_cond_mon2(
        input_data_gen_rep_various_cond_mon2, 
        new_var_v_cond='total_thefts_c', 
        ori_var_v_cond='total_thefts', 
        var_con_v_cond='month', 
        con_v_cond1=2, 
        multiple1_v_cond=2, 
        multiple2_v_cond=3
    )
    assert list(df['total_thefts_c']) == [10, 40, 90, 120, 150] # asserting that with conditions it takes the appropiate values
    
def test__egen_rep_mon2(input_data_egen_rep_mon2):
    new_egen_var_m2 = 'totalpre'
    by_var_m2 = 'observ' 
    var_egen_sup_m2 = 'prethefts'
    cond_ege_var_m2 = 'month'
    ege_var_change_m2 = 'theftscoll'
    cond_ege_val_m2 = 4
    ege_scale_factor_m2 = 4
    
    expected_output = pd.DataFrame({'observ': [1, 1, 1, 1, 1],
                                    'month': [1, 2, 3, 4, 5],
                                    'prethefts': [1, 2, 3, 4, 5],
                                    'theftscoll': [1, 2, 3, 1.25, 5], # modified as necesarily
                                    'totalpre': [15, 15, 15, 15, 15]}) # created as per the cum sum of observ

    # Test the function output against the expected output
    assert expected_output.equals(_egen_rep_mon2(input_data_egen_rep_mon2, new_egen_var_m2, by_var_m2, var_egen_sup_m2, 
                                                 cond_ege_var_m2, ege_var_change_m2, cond_ege_val_m2, 
                                                 ege_scale_factor_m2))
    
def test_complex_gen_rep_mon2(input_data_complex_gen_rep_mon2):
    
    list_names_complex_m2 = ['Belgrano', 'Once', 'V. Crespo']
    var_rep_cond_complex_m2 = 'neighborhood'
    var_rep_complex_m2 = 'n_neighborhood'
    gen_var_complex_m2 = 'code2'
    var_cond_complex_m2 = 'month'
    list_names_complexb_m2 = ['belgrano', 'once', 'vcrespo']
    
    df = _complex_gen_rep_mon2(input_data_complex_gen_rep_mon2, list_names_complex_m2, var_rep_cond_complex_m2, var_rep_complex_m2, gen_var_complex_m2, var_cond_complex_m2, list_names_complexb_m2)
    
    # Assert the values in the 'n_neighborhood' column have been replaced based on the 'neighborhood' column and 'list_names_complex_m2' and 'range_complex'.
    assert np.array_equal(df['n_neighborhood'], [1, 2, 3, 1, 2])
    
    # Assert the new 'code2' column has been generated correctly based on the 'month', 'n_neighborhood', and 'scale_complex'.
    assert np.array_equal(df['code2'], [0, 0, 0, 1000, 2000])
    
    # Assert the values in the 'belgrano', 'once', and 'vcrespo' columns have been correctly replaced based on the 'n_neighborhood' column and 'list_names_complex_m2'.
    assert np.array_equal(df['belgrano'], [1, 0, 0, 1, 0])
    assert np.array_equal(df['once'], [0, 1, 0, 0, 1])
    assert np.array_equal(df['vcrespo'], [0, 0, 1, 0, 0])
    

def test_gen_rep_simple_mon2( # Test using our own data set
    input_data_gen_rep_simple_mon2,
):
    (
        df, 
        var_gen_simple, 
        var_cond_simple,
        original_val_simple,
        cond_simple,
        value_final_simple
    ) = input_data_gen_rep_simple_mon2

    # Call the function being tested
    new_df = _gen_rep_simple_mon2(
        df, 
        var_gen_simple, 
        var_cond_simple,
        original_val_simple,
        cond_simple,
        value_final_simple
    )

    # Test that the values were replaced as we wanted
    assert all(new_df.loc[new_df[var_cond_simple]==cond_simple, var_gen_simple]==value_final_simple)



def test_gen_based_variouslists_mon2( # Test using our own data set
        input_data_gen_based_variouslists_mon2,
):
    (
        df, 
        list_names_place, 
        list_names_month,
        list_names_variouslists
    ) = input_data_gen_based_variouslists_mon2

    # Define the new data frame
    new_df = _gen_based_variouslists_mon2(df, list_names_place, list_names_month, list_names_variouslists)

    # Declare the columns to be changed
    expected_columns = ["mbelgapr", "mbelgmay", "mbelgjun", "mbelgjul", "mbelgago", "mbelgsep", "mbelgoct", "mbelgnov", "mbelgdec", 
                        "monceapr", "moncemay", "moncejun", "moncejul", "monceago", "moncesep", "monceoct", "moncenov", "moncedec", 
                        "mvcreapr", "mvcremay", "mvcrejun", "mvcrejul", "mvcreago", "mvcressep", "mvcreoct", "mvcrenov", "mvcredec"]
    
    # Assert that these new columns exist in the new_df
    assert all(col in new_df.columns for col in expected_columns)

############################################## PART 3 ########################################################################################################################

def test_gen_rep_3cond_mon3(
    input_data_gen_rep_3cond_mon3                         
):
    (
        df,
        initial_val_3cond,
        global_replace_val_3cond,
        gen_var_3cond,
        col1_3cond,
        col2_3cond,
        col3_3cond
    ) = input_data_gen_rep_3cond_mon3


    # Apply the function to the sample dataframe
    new_df = _gen_rep_3cond_mon3(df, gen_var_3cond, col1_3cond, col2_3cond, col3_3cond, initial_val_3cond, global_replace_val_3cond)

    # Check if the new column has been added
    assert gen_var_3cond in new_df.columns

    # Check if the new column has been assigned the correct values
    expected_output = pd.Series([1, 1, 0, 1])
    pd.testing.assert_series_equal(new_df[gen_var_3cond], expected_output, check_dtype=False)
    

def test_gen_multiplevariables_listbased( # Test using our own data set
        input_data_gen_multiplevariables_listbased,
):
    (
        df,
        list_values,
        list_names_3_variables,
        list_names_data3_general
    ) = input_data_gen_multiplevariables_listbased

    # Define the new data frame
    new_df = _gen_multiplevariables_listbased(df, list_values, list_names_3_variables, list_names_data3_general)

    # Declare the columns to be changed
    expected_columns = ['public_building_or_embassy_p', 'public_building_or_embassy_1_p', 'public_building_or_embassy_cuad2p', 'n_public_building_or_embassy_p', 'n_public_building_or_embassy_1_p', 'n_public_building_or_embassy_cuad2p',
                        'gas_station_p', 'gas_station_1_p', 'gas_station_cuad2p', 'n_gas_station_p', 'n_gas_station_1_p', 'n_gas_station_cuad2p',
                        'bank_p', 'bank_1_p', 'bank_cuad2p', 'n_bank_p', 'n_bank_1_p', 'n_bank_cuad2p',
                        'all_locations_p', 'all_locations_1_p', 'all_locations_cuad2p', 'n_all_locations_p', 'n_all_locations_1_p', 'n_all_locations_cuad2p']
    
    # Assert that these new columns exist in the new_df
    assert all(col in new_df.columns for col in expected_columns)




