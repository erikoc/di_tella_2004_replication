import pandas as pd
import pyreadstat
import pytest
from di_tella_2004_replication.config import SRC
from di_tella_2004_replication.data_management.clean_MonthlyPanel import (
    _clean_column_names_mon,
    _complex_variable_generator,
    _egenerator_sum,
    _generate_dummy_variables_fixed_extension,
    _generate_similar_named_variables,
    _generate_variables_based_on_various_lists,
    _generate_variables_different_conditions,
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
        df,
        "bigger than",
        "conditional_var",
        "var_to_replace",
        25,
        1,
    )

    # Check that the variable values were replaced correctly
    assert list(new_df["var_to_replace"]) == [0, 0, 1, 1, 1]


@pytest.fixture()
def input_data_generate_similar_named_variables():
    df = pd.DataFrame(
        {"cuad0": [1, 1, 1], "cuad1": [1, 1, 1], "fixed": [1, 2, 3]},
    )
    original_variables = ["cuad0", "cuad1"]
    fixed_variable = "fixed"
    return (df, original_variables, fixed_variable)


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
