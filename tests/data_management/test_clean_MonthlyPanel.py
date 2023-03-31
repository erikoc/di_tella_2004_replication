import pandas as pd
import pyreadstat
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_MonthlyPanel import (
    _clean_column_names_mon,
    _complex_variable_generator,
    _egenerator_sum,
    _generate_dummy_variables_fixed_extension,
    _generate_multiplevariables_listbased,
    _generate_similar_named_variables,
    _generate_total_thefts2_mon,
    _generate_variable_based_on_three_or_conditions,
    _generate_variable_basedon_doublelist,
    _generate_variables_based_on_list_and_loop,
    _generate_variables_based_on_various_lists,
    _generate_variables_different_conditions,
    _generate_variables_original_with_no_value_after_replace,
    _generate_variables_specificrule_list,
    _generate_various_variables_conditional,
    _rep_variables_based_on_condition,
)

"_clean_column_names_mon"


@pytest.fixture()
def original_data():
    data, meta = pyreadstat.read_dta(SRC / "data" / "MonthlyPanel.dta")
    return data


@pytest.fixture()
def input_data_generate_similar_named_variables():
    df = pd.DataFrame(
        {
            "cuad0": [1, 2, 3],
            "cuad1": [4, 5, 6],
            "cuad2": [7, 8, 9],
            "cuad3": [10, 11, 12],
            "cuad4": [13, 14, 15],
            "post": [25, 26, 27],
        },
    )
    original_variables = (["cuad0", "cuad1", "cuad2", "cuad3", "cuad4"],)
    fixed_variable = "post"
    return (
        df,
        original_variables,
        fixed_variable,
    )


def test_clean_column_names_mon(original_data):
    new_df = _clean_column_names_mon(original_data)
    list_var = [
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

    assert all(col in list_var for col in new_df.columns)


def test_generate_dummy_variables_fixed_extension():
    df = pd.DataFrame(
        {"month": [5, 6, 7, 8, 9], "variable_conditional_extension": [5, 6, 7, 8, 9]},
    )

    new_df = _generate_dummy_variables_fixed_extension(
        df,
        "variable_conditional_extension",
        "month{}",
        range_loop=range(5, 13),
        original_value_variable=0,
        final_value_variable=1,
    )

    # Check that new columns were added
    expected_columns = [
        "month5",
        "month6",
        "month7",
        "month8",
        "month9",
        "month10",
        "month11",
        "month12",
    ]
    assert set(expected_columns).issubset(set(new_df.columns))


def test_rep_variables_based_on_condition():
    # Create a simple test DataFrame
    df = pd.DataFrame(
        {"conditional_var": [10, 20, 30, 40, 50], "var_to_replace": [0, 0, 0, 0, 0]},
    )

    # Call the function being tested
    new_df = _rep_variables_based_on_condition(
        df, "bigger than", "conditional_var", "var_to_replace", 25, 1,
    )

    # Check that the variable values were replaced correctly
    assert list(new_df["var_to_replace"]) == [0, 0, 1, 1, 1]
    
    
    
@pytest.fixture()
def input_data_generate_similar_named_variables():
    df = pd.DataFrame({
        "cuad0": [1, 1, 1],
        "cuad1": [1, 1, 1], 
        "fixed": [1, 2, 3]   
    },
    )
    original_variables=["cuad0", "cuad1"]
    fixed_variable="fixed"
    return (
        df,
        original_variables,
        fixed_variable
    )


def test_generate_similar_named_variables(input_data_generate_similar_named_variables):
    (
        df,
        original_variables,
        fixed_variable,
    ) = input_data_generate_similar_named_variables

    # Call the function being tested
    new_df = _generate_similar_named_variables(
        df,
        original_variables,
        fixed_variable,
    )

    # Test that new columns were added
    list_var = ["cuad0p", "cuad1p"]
    # Assert that these above are in the columns
    assert all(col in new_df.columns for col in list_var)


"_generate_variables_based_on_list_and_loop"


@pytest.fixture()
def input_data_generate_variables_based_on_list_and_loop():
    df = pd.DataFrame(
        {
            "a": [
                1,
                0,
            ],
            "b": [
                1,
                0,
            ],
        },
    )
    condition_type = ("condition from another variable",)
    new_generated_variable = "code"
    list_a = ["a", "b"]
    list_b = range(1, 3)
    new_original_value = 4
    return (
        df,
        condition_type,
        new_generated_variable,
        list_a,
        list_b,
        new_original_value,
    )


def test_generate_variables_based_on_list_and_loop(
    input_data_generate_variables_based_on_list_and_loop,
):
    (
        df,
        condition_type,
        new_generated_variable,
        list_a,
        list_b,
        new_original_value,
    ) = input_data_generate_variables_based_on_list_and_loop

    # Call the function being tested
    new_df = _generate_variables_based_on_list_and_loop(
        df,
        condition_type,
        new_generated_variable,
        list_a,
        list_b,
        new_original_value,
    )

    # Assert the correct change in the value for the 'code'
    expected_result = [1, list_b[0]]
    assert list(new_df[new_generated_variable]) == expected_result


"_generate_variable_basedon_doublelist"


@pytest.fixture()
def input_data_generate_variable_basedon_doublelist():
    df = pd.DataFrame({
            'x': [1, 2, 3, 4],
            'y': [0.5, 0.6, 0.7, 0.8],
            'z': [2, 3, 4, 5]
    })
    list_generated_variable = ["var1", "var2"]
    list_original_variable = ["x", "y"]
    fixed_variable = "z"
    return (df, list_generated_variable, list_original_variable, fixed_variable)


def test_generate_variable_basedon_doublelist(
    input_data_generate_variable_basedon_doublelist,
):
    (
        df,
        list_generated_variable,
        list_original_variable,
        fixed_variable,
    ) = input_data_generate_variable_basedon_doublelist

    # Call the function being tested
    new_df = _generate_variable_basedon_doublelist(
        df,
        list_generated_variable,
        list_original_variable,
        fixed_variable,
    )

    # Assert the correct change in the value for "ara1" and "bera1"
    assert list(new_df["var1"]) == [2.0, 6.0, 12.0, 20.0]
    assert list(new_df["var2"]) == [1.0, 1.8, 2.8, 4.0]


"_generate_total_thefts2_mon"


@pytest.fixture()
def input_data_generate_total_thefts2_mon():
    df = pd.DataFrame(
        {
            'variable_complex': [71, 72, 72, 73, 74, 73, 72, 72, 73],
            'total_thefts': [3, 2, 5, 1, 2, 4, 5, 3, 6]     
        },
    )
    variable_complex_condition = "variable_complex"
    return (
        df,
        variable_complex_condition,
    )


def test_generate_total_thefts2_mon(input_data_generate_total_thefts2_mon):
    (
        df,
        variable_complex_condition,
    ) = input_data_generate_total_thefts2_mon

    # Call the function being tested
    new_df = _generate_total_thefts2_mon(
        df,
        variable_complex_condition,
    )

    # Assert the correct change in the value for "ara1" and "bera1"
    assert list(new_df["total_thefts2"]) == [3, pd.NA, pd.NA, pd.NA, 2, pd.NA, pd.NA, pd.NA, pd.NA]


"_generate_variables_different_conditions"


@pytest.fixture()
def input_data_generate_variables_different_conditions():
    df = pd.DataFrame({"var1": [1, 2, 3, 4, 5], "var_con": [5, 7, 8, 10, 12]})
    new_variable_v_cond = "new"
    ori_variable_v_cond = "var1"
    variable_conditional_v_cond = "var_con"
    multiple1_v_cond = 0.5  # changed respective to original value for the function
    multiple2_v_cond = 0.75  # changed respective to original value for the function
    return (
        df,
        new_variable_v_cond,
        ori_variable_v_cond,
        variable_conditional_v_cond,
        multiple1_v_cond,
        multiple2_v_cond,
    )


def test_generate_variables_different_conditions(
    input_data_generate_variables_different_conditions,
):
    (
        df,
        new_variable_v_cond,
        ori_variable_v_cond,
        variable_conditional_v_cond,
        multiple1_v_cond,
        multiple2_v_cond,
    ) = input_data_generate_variables_different_conditions

    # Call the function being tested
    new_df = _generate_variables_different_conditions(
        df,
        new_variable_v_cond,
        ori_variable_v_cond,
        variable_conditional_v_cond,
        multiple1_v_cond=multiple1_v_cond,
        multiple2_v_cond=multiple2_v_cond,
    )

    # Test that new variable is generated
    assert "new" in new_df.columns

    # Test that new variable is calculated correctly
    tolerance = 1e-9  # Adjust this value as needed
    assert new_df.loc[0, "new"] == pytest.approx(0.5, rel=tolerance, abs=tolerance)


"_generate_variables_original_with_no_value_after_replace"


@pytest.fixture()
def input_data_generate_variables_original_with_no_value_after_replace():
    df = pd.DataFrame({"Car": [7, 8, 9], "Dar": [10, 11, 12]})
    list_variables_original_novalue = ["Ara", "Bar"]
    conditional_variable_for_novalue = "Car"
    equalizing_variable = "Dar"
    return (
        df,
        list_variables_original_novalue,
        conditional_variable_for_novalue,
        equalizing_variable,
    )


def test_generate_variables_original_with_no_value_after_replace(
    input_data_generate_variables_original_with_no_value_after_replace,
):
    (
        df,
        list_variables_original_novalue,
        conditional_variable_for_novalue,
        equalizing_variable,
    ) = input_data_generate_variables_original_with_no_value_after_replace

    # Call the function being tested
    new_df = _generate_variables_original_with_no_value_after_replace(
        df,
        list_variables_original_novalue,
        conditional_variable_for_novalue,
        equalizing_variable,
    )

    # Test that new variable is generated
    assert "Ara" in new_df.columns
    assert "Bar" in new_df.columns

    # Test the correct values
    assert new_df["Ara"].to_list() == [10, None, None]
    assert new_df["Bar"].to_list() == [None, 11, 12]


"_egenerator_sum"


@pytest.fixture()
def input_data_egenerator_sum():
    df = pd.DataFrame(
        {
            "by": [1, 1, 2, 2, 2],
            "filter": [3, 4, 5, 6, 7],
            "conditional": [8, 8, 9, 9, 9],  # Fix the typo here
            "egenerator": [10, 11, 12, 13, 14],
        },
    )
    new_egenerator_variable = "variable_new"
    by_variable = "by"
    variable_egenerator_filter = "filter"
    condional_egenerator_variable = "conditional"  # And here
    egenerator_variable_tochange = "egenerator"
    return (
        df,
        new_egenerator_variable,
        by_variable,
        variable_egenerator_filter,
        condional_egenerator_variable,
        egenerator_variable_tochange,
    )


def test_egenerator_sum(input_data_egenerator_sum):
    (
        df,
        new_egenerator_variable,
        by_variable,
        variable_egenerator_filter,
        condional_egenerator_variable,
        egenerator_variable_tochange,
    ) = input_data_egenerator_sum

    # Call the function being tested
    new_df = _egenerator_sum(
        df,
        new_egenerator_variable,
        by_variable,
        variable_egenerator_filter,
        condional_egenerator_variable,
        egenerator_variable_tochange,
    )

    # Assert that the new variable has been created and has the correct values
    assert new_df[new_egenerator_variable].equals(pd.Series([7, 7, 18, 18, 18]))


"_complex_variable_generator"


@pytest.fixture()
def input_data_complex_variable_generator():
    df = pd.DataFrame(
        {
            "month": [1, 2, 3, 4, 5],
            "neighborhood": ["Belgrano", "Once", "V. Crespo", "Belgrano", "Once"],
            "n_neighborhood": [0, 0, 0, 0, 0],
        },
    )
    list_names_complexa = ["Belgrano", "Once", "V. Crespo"]
    variable_replace_condition_complex = "neighborhood"
    variable_replace_complex = "n_neighborhood"
    generate_var_complex = "code2"
    variable_condition_complex = "month"
    list_names_complexb = ["belgrano", "once", "vcrespo"]
    return (
        df,
        list_names_complexa,
        variable_replace_condition_complex,
        variable_replace_complex,
        generate_var_complex,
        variable_condition_complex,
        list_names_complexb,
    )


def test_complex_variable_generator(input_data_complex_variable_generator):
    (
        df,
        list_names_complexa,
        variable_replace_condition_complex,
        variable_replace_complex,
        generate_var_complex,
        variable_condition_complex,
        list_names_complexb,
    ) = input_data_complex_variable_generator

    # Call the function being tested
    new_df = _complex_variable_generator(
        df,
        list_names_complexa,
        variable_replace_condition_complex,
        variable_replace_complex,
        generate_var_complex,
        variable_condition_complex,
        list_names_complexb,
    )

    # Assert that the generated variable is calculated correctly
    expected_generate_var = pd.Series([1001, 2002, 3003, 1004, 2005], name="code2")
    pd.testing.assert_series_equal(new_df[generate_var_complex], expected_generate_var)


"_generate_variables_based_on_various_lists"


@pytest.fixture()
def input_data_generate_variables_based_on_various_lists():
    df = pd.DataFrame(
        {
            "month4": [1, 2, 3, 4, 5],
            "month5": [6, 7, 8, 9, 10],
            "month6": [11, 12, 13, 14, 15],
            "month7": [16, 17, 18, 19, 20],
            "month8": [21, 22, 23, 24, 25],
            "month9": [26, 27, 28, 29, 30],
            "month10": [31, 32, 33, 34, 35],
            "month11": [36, 37, 38, 39, 40],
            "month12": [41, 42, 43, 44, 45],
            "belgrano": [1, 2, 3, 4, 5],
            "once": [6, 7, 8, 9, 10],
            "vcrespo": [11, 12, 13, 14, 15],
        },
    )
    list_names_variouslists = ["belgrano", "once", "vcrespo"]
    return (df, list_names_variouslists)


def test_generate_variables_based_on_various_lists(
    input_data_generate_variables_based_on_various_lists,
):

    (df, list_names_variouslists) = input_data_generate_variables_based_on_various_lists

    # Call the function being tested
    new_df = _generate_variables_based_on_various_lists(df, list_names_variouslists)

    # Quick asserts
    assert list(new_df["mbelgapr"]) == [1, 4, 9, 16, 25]
    assert list(new_df["monceapr"]) == [6, 14, 24, 36, 50]
    assert list(new_df["mvcreapr"]) == [11, 24, 39, 56, 75]


"_generate_variable_based_on_three_or_conditions"


@pytest.fixture()
def input_data_generate_variable_based_on_three_or_conditions():
    df = pd.DataFrame(
        {
            "public_building_or_embassy": [0, 1, 0, 0],
            "gas_station": [1, 0, 0, 1],
            "bank": [0, 1, 0, 0],
        },
    )
    generate_variable_three_conditions = "all_locations"
    column1_three_conditions = "public_building_or_embassy"
    column2_three_conditions = "gas_station"
    column3_three_conditions = "bank"
    return (
        df,
        generate_variable_three_conditions,
        column1_three_conditions,
        column2_three_conditions,
        column3_three_conditions,
    )


def test_generate_variable_based_on_three_or_conditions(
    input_data_generate_variable_based_on_three_or_conditions,
):
    (
        df,
        generate_variable_three_conditions,
        column1_three_conditions,
        column2_three_conditions,
        column3_three_conditions,
    ) = input_data_generate_variable_based_on_three_or_conditions

    # Apply the function to the sample dataframe
    new_df = _generate_variable_based_on_three_or_conditions(
        df,
        generate_variable_three_conditions,
        column1_three_conditions,
        column2_three_conditions,
        column3_three_conditions,
    )

    # Check if the new column has been added
    assert generate_variable_three_conditions in new_df.columns

    # Check if the new column has been assigned the correct values
    expected_output = pd.Series([1, 1, 0, 1])
    pd.testing.assert_series_equal(
        new_df[generate_variable_three_conditions],
        expected_output,
        check_dtype=False,
    )


@pytest.fixture()
def input_data_generate_multiplevariables_listbased():
    df = pd.DataFrame(
        {
            "var1": [1, 2, 3],
            "var2": [4, 5, 6],
            "var3": [7, 8, 9],
            "var4": [10, 11, 12],
        },
    )
    return df


def test_generate_multiplevariables_listbased(
    input_data_generate_multiplevariables_listbased,
):
    list_names_multi_variables = ["var1", "var2", "var3", "var4"]
    list_names_multi_general = ["var2", "var3", "var4", "var1"]

    expected_df = pd.DataFrame(
        {
            "var1": [1, 2, 3],
            "var2": [4, 5, 6],
            "var3": [7, 8, 9],
            "var4": [10, 11, 12],
            "public_building_or_embassy_p": [4, 10, 18],
            "public_building_or_embassy_1_p": [16, 40, 72],
            "public_building_or_embassy_cuad2p": [40, 100, 180],
            "n_public_building_or_embassy_p": [24, 50, 72],
            "n_public_building_or_embassy_1_p": [96, 200, 288],
            "n_public_building_or_embassy_cuad2p": [240, 500, 720],
            "gas_station_p": [4, 10, 18],
            "gas_station_1_p": [16, 40, 72],
            "gas_station_cuad2p": [40, 100, 180],
            "n_gas_station_p": [24, 50, 72],
            "n_gas_station_1_p": [96, 200, 288],
            "n_gas_station_cuad2p": [240, 500, 720],
            "bank_p": [4, 10, 18],
            "bank_1_p": [16, 40, 72],
            "bank_cuad2p": [40, 100, 180],
            "n_bank_p": [24, 50, 72],
            "n_bank_1_p": [96, 200, 288],
            "n_bank_cuad2p": [240, 500, 720],
            "all_locations_p": [4, 10, 18],
            "all_locations_1_p": [16, 40, 72],
            "all_locations_cuad2p": [40, 100, 180],
            "n_all_locations_p": [24, 50, 72],
            "n_all_locations_1_p": [96, 200, 288],
            "n_all_locations_cuad2p": [240, 500, 720],
        },
    )

    output_df = _generate_multiplevariables_listbased(
        df=input_data_generate_multiplevariables_listbased,
        list_names_multi_variables=list_names_multi_variables,
        list_names_multi_general=list_names_multi_general,
    )

    assert expected_df.equals(output_df)


"_generate_various_variables_conditional"


@pytest.fixture()
def input_data_generate_various_variables_conditional():
    df = pd.DataFrame({"month": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    list_genenerate_variables_conditional = ["post1", "post2", "post3"]
    conditional_variable = "month"
    return (df, list_genenerate_variables_conditional, conditional_variable)


def test_generate_various_variables_conditional(
    input_data_generate_various_variables_conditional,
):
    (
        df,
        list_genenerate_variables_conditional,
        conditional_variable,
    ) = input_data_generate_various_variables_conditional

    # Call the function being tested
    new_df = _generate_various_variables_conditional(
        df,
        list_genenerate_variables_conditional,
        conditional_variable,
    )

    # Quick asserts
    assert list(new_df["post1"]) == [0, 0, 0, 0, 1, 1, 1, 1, 1, 1]
    assert list(new_df["post2"]) == [0, 0, 0, 0, 0, 1, 1, 1, 1, 1]
    assert list(new_df["post3"]) == [0, 0, 0, 0, 0, 0, 1, 1, 1, 1]


"_generate_variables_specificrule_list"


@pytest.fixture()
def input_data_generate_variables_specificrule_list():
    return pd.DataFrame(
        {
            "post1": [1, 2, 3, 4, 5],
            "post2": [6, 7, 8, 9, 10],
            "post3": [11, 12, 13, 14, 15],
            "jewish_inst": [0.2, 0.4, 0.6, 0.8, 1.0],
            "jewish_inst_one_block_away_1": [0.3, 0.6, 0.9, 1.2, 1.5],
            "cuad2": [0.4, 0.8, 1.2, 1.6, 2.0],
        },
    )


def test__generate_variables_specificrule_list(
    input_data_generate_variables_specificrule_list,
):

    list_variable_generate_specific = [
        "jewish_inst",
        "jewish_inst_one_block_away_1",
        "cuad2",
    ]
    list_variable_extisting_specific = ["post1", "post2", "post3"]
    list_new_variable_specific = [
        "one_jewish_inst_1_p",
        "one_jewish_inst_one_block_away_1_p",
        "one_cuad2p",
        "two_jewish_inst_1_p",
        "two_jewish_inst_one_block_away_1_p",
        "two_cuad2p",
        "three_jewish_inst_1_p",
        "three_jewish_inst_one_block_away_1_p",
        "three_cuad2p",
    ]
    range_specific_loop = [0, 3, 6]

    expected_columns = (
        input_data_generate_variables_specificrule_list(
            input_data_generate_variables_specificrule_list.columns,
        )
        + list_new_variable_specific
    )
    expected_shape = (5, len(expected_columns))

    result = _generate_variables_specificrule_list(
        input_data_generate_variables_specificrule_list,
        list_variable_generate_specific,
        list_variable_extisting_specific,
        list_new_variable_specific,
        range_specific_loop,
    )

    assert list(result.columns) == expected_columns
    assert result.shape == expected_shape
