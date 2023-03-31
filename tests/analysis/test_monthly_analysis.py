import pandas as pd
import pytest
from di_tella_2004_replication.analysis.monthly_panel_regression import (
    areg_clus,
    areg_clus_abs,
    areg_triple,
    poisson_reg,
    reg_robust,
)
from di_tella_2004_replication.analysis.monthly_panel_stats import WelchTest

"MONTHLY"

# Stats


@pytest.fixture()
def input_data_WelchTest():
    Data = pd.DataFrame(
        {"code": [1, 1, 2, 2], "month": [1, 2, 1, 2], "total_thefts": [10, 12, 8, 9]},
    )
    code1 = 1
    code2 = 2
    return Data, code1, code2


def test_WelchTest(input_data_WelchTest):
    Data, code1, code2 = input_data_WelchTest
    # run the WelchTest
    t, p = WelchTest(Data, code1, code2)
    # we run a Welch test before hand with our above parameters and we get the following expected values
    expected_t = 2.236
    expected_p = 0.199
    # check if the calculated t and p values match the our before hand calculated values
    assert round(t, 3) == expected_t
    assert round(p, 3) == expected_p


# Regressions


@pytest.fixture()
def input_data_areg_triple():
    Data = pd.DataFrame(
        {  # The variables in question only have 0 and 1 values except for y and x
            "loga": [1, 0, 0, 1, 0],
            "logb": [0, 1, 1, 0, 0],
            "logc": [0, 0, 0, 1, 1],
            "fe": [1, 0, 1, 0, 1],
            "y": [1, 2, 3, 4, 5],
            "x": [6, 7, 8, 9, 10],
        },
    )
    variable_loga = "loga"
    variable_logb = "logb"
    variable_logc = "logc"
    a = 1
    variable_fe = "fe"
    variable_y = "y"
    variable_x = "x"
    return (
        Data,
        variable_loga,
        variable_logb,
        variable_logc,
        a,
        variable_fe,
        variable_y,
        variable_x,
    )


def test_areg_triple(input_data_areg_triple):
    (
        Data,
        variable_loga,
        variable_logb,
        variable_logc,
        a,
        variable_fe,
        variable_y,
        variable_x,
    ) = input_data_areg_triple

    # Implementing the regression
    result = areg_triple(
        Data,
        variable_loga,
        variable_logb,
        variable_logc,
        a,
        variable_fe,
        variable_y,
        variable_x,
    )

    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[0], 2) == -5.0
    assert round(result.params[1], 2) == 1.0


@pytest.fixture()
def input_data_reg_robust():
    Data = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": [1, 3, 2, 5, 10]})
    variable_y = "y"
    variable_x = "x"
    return Data, variable_y, variable_x


def test_reg_robust(input_data_reg_robust):
    Data, variable_y, variable_x = input_data_reg_robust

    # Implementing the regression
    result = reg_robust(Data, variable_y, variable_x)

    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[0], 3) == 1.399


@pytest.fixture()
def input_data_areg_clus():
    Data = pd.DataFrame(
        {"x": [1, 2, 3, 4, 5], "y": [1, 3, 2, 5, 10], "observ": [1, 2, 3, 4, 5]},
    )
    variable_y = "y"
    variable_x = "x"
    return Data, variable_y, variable_x


def test_areg_clus(input_data_areg_clus):
    Data, variable_y, variable_x = input_data_areg_clus

    # Implementing the regression
    result = areg_clus(Data, variable_y, variable_x)

    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == 2.0


@pytest.fixture()
def input_data_areg_clus_abs():
    # generate a sample dataset for testing
    Data = pd.DataFrame(
        {
            "y": [1, 2, 3, 4],
            "x": [4, 5, 6, 7],
            "z": [7, 8, 9, 10],
            "dummy": [1, 1, 2, 2],
        },
    )
    y_variable = "y"
    x_variable = "x"
    dummy_variable = "dummy"
    drop_subset = "z"
    return Data, drop_subset, y_variable, x_variable, dummy_variable


def test_areg_clus_abs(input_data_areg_clus_abs):
    Data, drop_subset, y_variable, x_variable, dummy_variable = input_data_areg_clus_abs

    # Implementing the regression
    result = areg_clus_abs(Data, drop_subset, y_variable, x_variable, dummy_variable)

    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[1], 2) == -3.0


@pytest.fixture()
def input_data_poisson_reg():
    Data = pd.DataFrame(
        {
            "y": [1, 2, 0, 4, 1, 3, 2, 0],
            "x1": [1, 2, 3, 4, 5, 6, 7, 8],
            "x2": [0, 1, 0, 1, 0, 1, 0, 1],
            "index1": [1, 1, 1, 1, 2, 2, 2, 2],
            "index2": [1, 1, 2, 2, 1, 1, 2, 2],
        },
    )
    y_variable = "y"
    x_variable = ["x1", "x2"]
    index_variables = ["index1", "index2"]
    type_of_possion = "fixed effects"
    weight = None
    x_irra = ["x1", "x2"]
    return (
        Data,
        y_variable,
        x_variable,
        index_variables,
        type_of_possion,
        weight,
        x_irra,
    )


def test_poisson_reg(input_data_poisson_reg):
    (
        Data,
        y_variable,
        x_variable,
        index_variables,
        type_of_possion,
        weight,
        x_irra,
    ) = input_data_poisson_reg

    # Implementing the regression
    result = poisson_reg(
        Data,
        y_variable,
        x_variable,
        index_variables,
        type_of_possion,
        weight,
        x_irra,
    )

    # Assert it accomplishes our precalculated values rounded up
    assert round(result.params[0], 2) == 1.25
