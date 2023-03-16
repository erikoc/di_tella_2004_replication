"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd


def _clean_column_names(df):
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
        df.columns.str.replace("rob", "theft")
        .str.replace("day", "week_day")
        .str.replace("dia", "day")
        .str.replace("mes", "month")
        .str.replace("hor", "hour")
        .str.replace("mak", "brand")
        .str.replace("mak", "brand")
        .str.replace("mak", "brand")
        .str.replace("esq", "corner")
        .str.replace("observ", "block")
        .str.replace("barrio", "neighborhood")
        .str.replace("calle", "street")
        .str.replace("altura", "street_nr")
        .str.replace("institu1", "jewish_inst")
        .str.replace("institu3", "jewish_inst_one_block_away")
        .str.replace("distanci", "distance_to_jewish_inst")
        .str.replace("edpub", "public_building_or_embassy")
        .str.replace("estserv", "gas_station")
        .str.replace("banco", "bank")
        .str.replace("district", "census_district")
        .str.replace("frcensal", "census_tract")
        .str.replace("edad", "av_age")
        .str.replace("mujer", "female_rate")
        .str.replace("propiet", "ownership_rate")
        .str.replace("tamhogar", "av_hh_size")
        .str.replace("nohacinado", "non_overcrowd_rate")
        .str.replace("nobi", "non_unmet_basic_needs_rate")
        .str.replace("educjefe", "av_hh_head_schooling")
        .str.replace("ocupado", "employment_rate")
    )
    return df


def _convert_dtypes(df, float_cols=None):
    """Converts specified columns in a pandas DataFrame to float type. Sets the 'block'
    column as the DataFrame index.

    Parameters:
        df (pandas.DataFrame): The pandas DataFrame containing the data to be converted.
        float_cols (list of str, optional): A list of the column names to be converted to float. Defaults
        to a list of theft columns and their values.

    Returns:
        pandas.DataFrame: The input DataFrame with the specified columns converted to float type
        and the 'block' column set as the index.

    """
    if not isinstance(float_cols, list) and float_cols is not None:
        raise TypeError("float_cols must be a list or None")

    if float_cols is None:
        float_cols = [f"theft{i}" for i in range(1, 23)] + [
            f"theft{i}val" for i in range(1, 23)
        ]
    df = df.convert_dtypes()
    df = df.set_index("block")
    df[float_cols] = df[float_cols].astype(float)

    return df


def _compute_theft_data(theft_data, month_dict):
    """Transforms the theft_data DataFrame by setting certain columns to 0.25, creating
    new columns based on conditions on existing columns, and computing totals and
    differences between totals.

    Parameters:
        theft_data (pandas.DataFrame): The DataFrame to be transformed.
        month_dict (dict): A dictionary mapping month names to integers.

    Returns:
        pandas.DataFrame: The transformed DataFrame.

    """
    for key, value in month_dict.items():
        for i in range(1, 23):
            theft_data.loc[theft_data[f"theft{i}corner"] == 1, f"theft{i}"] = 0.25
            common_conditions = (theft_data[f"theft{i}"] != 0) & (
                theft_data[f"theft{i}month"] == key
            )

            for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:
                theft_data[f"theft_{suffix}{i}{value}"] = np.where(
                    common_conditions
                    & (theft_data[f"theft{i}val"].between(8403.826, 100000))
                    if suffix == "hv"
                    else (theft_data[f"theft{i}val"].between(0, 8403.826))
                    if suffix == "lv"
                    else (theft_data[f"theft{i}hour"] <= 10)
                    | (theft_data[f"theft{i}hour"] > 22)
                    if suffix == "night"
                    else (
                        theft_data[f"theft{i}hour"].between(10, 22, inclusive="right")
                    )
                    if suffix == "day"
                    else (theft_data[f"theft{i}day"] <= 5)
                    if suffix == "weekday"
                    else (theft_data[f"theft{i}day"] > 5),
                    theft_data[f"theft{i}"],
                    0,
                )

        for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:
            col_regex = f"theft_{suffix}\\d+{value}"
            tot_col = f"tot_theft_{suffix}{key}"
            theft_data[tot_col] = theft_data.filter(regex=col_regex).sum(axis=1)

        theft_data[f"dif_hv_lv{key}"] = (
            theft_data[f"tot_theft_hv{key}"] - theft_data[f"tot_theft_lv{key}"]
        )
        theft_data[f"dif_night_day{key}"] = (
            theft_data[f"tot_theft_night{key}"] - theft_data[f"tot_theft_day{key}"]
        )
        theft_data[f"dif_weekday_weekend{key}"] = (
            theft_data[f"tot_theft_weekday{key}"]
            - theft_data[f"tot_theft_weekend{key}"]
        )

    return theft_data


def _create_panel_data(
    ind_char_data: pd.DataFrame,
    theft_data: pd.DataFrame,
) -> pd.DataFrame:
    """Merges individual and theft data by block and month.

    Args:
    - ind_char_data (pd.DataFrame): DataFrame containing individual characteristics data by block.
    - theft_data (pd.DataFrame): DataFrame containing theft data by block and month.

    Returns:
    - pd.DataFrame: Merged DataFrame with individual characteristics and theft data by block and month.

    """
    # Reshape the theft data using wide_to_long
    theft_data = pd.wide_to_long(
        theft_data,
        stubnames=[
            "tot_theft_hv",
            "tot_theft_lv",
            "dif_hv_lv",
            "tot_theft_night",
            "tot_theft_day",
            "dif_night_day",
            "tot_theft_weekday",
            "tot_theft_weekend",
            "dif_weekday_weekend",
        ],
        i=["block"],
        j="month",
    )

    # Reset the index and column names of the reshaped DataFrame
    theft_data = theft_data.reset_index(names=["block", "month"])

    # Merge the individual characteristics and theft data by block
    crime_by_block_panel = pd.merge(ind_char_data, theft_data, how="left", on=["block"])

    return crime_by_block_panel


def process_crimebyblock(df):
    """Processes the crime data in the given DataFrame, `df`, and returns a panel data
    structure with information on theft and individual characteristics by block and
    month.

    Args:
        df (pandas.DataFrame): A DataFrame containing crime data.

    Returns:
        pandas.DataFrame: A panel data structure with information on theft and
        individual characteristics by block and month.

    """
    month_dict = {
        4: "abr",
        5: "may",
        6: "jun",
        7: "jul",
        8: "ago",
        9: "sept",
        10: "oct",
        11: "nov",
        12: "dic",
    }

    df = _clean_column_names(df)
    df = _convert_dtypes(df)

    # Get columns starting with "ro"
    theft_data = df.loc[:, df.columns.str.startswith("theft")]

    # Get columns that don't start with "ro"
    ind_char_data = df[[col for col in df.columns if not col.startswith("theft")]]

    theft_data = _compute_theft_data(df, month_dict)

    theft_cols = [col for col in theft_data.columns if col.startswith("theft")]
    theft_data = theft_data.drop(columns=theft_cols)
    theft_data = theft_data.reset_index(names=["block"])

    crime_by_block_panel = _create_panel_data(ind_char_data, theft_data)
    crime_by_block_panel = crime_by_block_panel.set_index(["block", "month"])

    return crime_by_block_panel
