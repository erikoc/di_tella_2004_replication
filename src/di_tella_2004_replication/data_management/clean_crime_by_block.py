"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd


def _clean_column_names_block(df):
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
        .str.replace("distrito", "census_district")
        .str.replace("frcensal", "census_tract")
        .str.replace("edad", "av_age")
        .str.replace("mujer", "female_rate")
        .str.replace("propiet", "ownership_rate")
        .str.replace("tamhogar", "av_hh_size")
        .str.replace("nohacinado", "non_overcrowd_rate")
        .str.replace("nonbi", "non_unmet_basic_needs_rate")
        .str.replace("educjefe", "av_hh_head_schooling")
        .str.replace("ocupado", "employment_rate")
    )
    return df


def _clean_column_names_weekly(df):
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
        df.columns.str.replace("observ", "block")
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
    )

    return df


def _convert_dtypes(df, float_cols=None, maxrange=24):
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
        float_cols = [f"theft{i}" for i in range(1, maxrange)] + [
            f"theft{i}val" for i in range(1, maxrange)
        ]
    df = df.convert_dtypes()
    df = df.set_index("block")
    df[float_cols] = df[float_cols].astype(float)

    return df


def _create_new_variables_ind(df):
    """Creates new variables in the input DataFrame based on existing variables.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: The input DataFrame with additional columns for unmet basic needs rate,
        overcrowding rate, and unemployment rate.

    """
    df["no_jewish_inst"] = 1 - df["jewish_inst"]
    df["unmet_basic_needs_rate"] = 1 - df["non_unmet_basic_needs_rate"]
    df["overcrowd_rate"] = 1 - df["non_overcrowd_rate"]
    df["unemployment_rate"] = 1 - df["employment_rate"]
    df.sort_values(
        by=["census_district", "census_tract", "no_jewish_inst"],
        ascending=[True, True, True],
        na_position="first",
    )

    return df


def _drop_repated_obs(df):
    """Drops duplicate observations from a DataFrame based on the "census_district" and
    "census_tract" variables.

    Args:
        df (pandas.DataFrame): The input DataFrame.

    Returns:
        pandas.DataFrame: A new DataFrame that contains only the unique combinations of "census_district" and
        "census_tract" variables.

    """
    df_unique = df.loc[
        (df["census_district"].shift() != df["census_district"])
        | (df["census_tract"].shift() != df["census_tract"])
    ]
    df_unique = df_unique.reset_index(drop=True)

    return df_unique


def _split_theft_data(theft_data, month, maxrange=24):
    """Splits the theft data into different categories based on specific conditions for
    a given month.

    Args:
        theft_data (pd.DataFrame): The theft data to split.
        month (int): The month to split the data for.

    Returns:
        pd.DataFrame: The split theft data.

    """
    for i in range(1, maxrange):
        common_conditions = (theft_data[f"theft{i}"] != 0) & (
            theft_data[f"theft{i}month"] == month
        )

        for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:
            condition = None

            if suffix == "hv":
                condition = common_conditions & theft_data[f"theft{i}val"].between(
                    8403.826,
                    100000,
                )
            elif suffix == "lv":
                condition = common_conditions & theft_data[f"theft{i}val"].between(
                    0,
                    8403.826,
                )
            elif suffix == "night":
                condition = common_conditions & (
                    (theft_data[f"theft{i}hour"] <= 10)
                    | (theft_data[f"theft{i}hour"] > 22)
                )
            elif suffix == "day":
                condition = common_conditions & theft_data[f"theft{i}hour"].between(
                    10,
                    22,
                    inclusive="right",
                )
            elif suffix == "weekday":
                condition = common_conditions & theft_data[f"theft{i}day"].between(1, 5)
            elif suffix == "weekend":
                condition = common_conditions & theft_data[f"theft{i}day"].between(6, 7)

            theft_data.loc[condition, f"theft_{suffix}{i}{month}"] = theft_data.loc[
                condition,
                f"theft{i}",
            ]
            theft_data.loc[~condition, f"theft_{suffix}{i}{month}"] = 0

    return theft_data


def _calculate_total_theft_by_suffix(theft_data, month):
    """Calculates the total theft by suffix for a given month.

    Args:
        theft_data (pd.DataFrame): A DataFrame containing theft data.
        month (Int): The month for which to calculate the total theft.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated total theft by suffix.

    """
    for suffix in ["hv", "lv", "night", "day", "weekday", "weekend"]:
        col_regex = f"theft_{suffix}\\d+{month}"
        tot_col = f"tot_theft_{suffix}{month}"
        theft_data[tot_col] = theft_data.filter(regex=col_regex).sum(axis=1)

    return theft_data


def _calculate_theft_differences(theft_data, month):
    """Calculates the differences in total theft by suffix for a given month.

    Args:
        theft_data (pd.DataFrame): A DataFrame containing theft data.
        month (Int): The month for which to calculate the differences.

    Returns:
        pd.DataFrame: A DataFrame containing the calculated differences in theft by suffix.

    """
    theft_data[f"dif_hv_lv{month}"] = (
        theft_data[f"tot_theft_hv{month}"] - theft_data[f"tot_theft_lv{month}"]
    )
    theft_data[f"dif_night_day{month}"] = (
        theft_data[f"tot_theft_night{month}"] - theft_data[f"tot_theft_day{month}"]
    )
    theft_data[f"dif_weekday_weekend{month}"] = (
        theft_data[f"tot_theft_weekday{month}"]
        - theft_data[f"tot_theft_weekend{month}"]
    )

    return theft_data


def _create_panel_data(ind_char_data, theft_data):
    """Merges individual and theft data by block and month.

    Args:
    - ind_char_data (pd.DataFrame): DataFrame containing individual characteristics data by block.
    - theft_data (pd.DataFrame): DataFrame containing theft data by block and month.

    Returns:
    - pd.DataFrame: Merged DataFrame with individual characteristics and theft data by block and month.

    """
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

    theft_data = theft_data.reset_index(names=["block", "month"])

    return pd.merge(ind_char_data, theft_data, how="left", on=["block"])


def _create_new_variables(df, time_variable, event_time):
    """This function takes a pandas DataFrame as input and creates new variables based
    on the existing columns of the DataFrame.

    Args:
    df (pandas.DataFrame): A pandas DataFrame containing the data for creating new variables.

    Returns:
    pandas.DataFrame: A pandas DataFrame with new variables added based on the existing columns of the input DataFrame. The new variables are:
    - jewish_inst_only_one_block_away: The difference between the number of Jewish institutions that are one block away and the number of Jewish institutions.
    - month_dummy: A dummy variable for each month in the input data.
    - post: A dummy variable that takes the value 1 if the month is greater than 7, and 0 otherwise.
    - treatment: A treatment dummy variable based on the "sameblock" and "post" columns.
    - treatment_1d: A treatment dummy variable based on the "oneblock" and "post" columns.
    - treatment_2d: A treatment dummy variable that takes the value 1 if the distance between the observation and the Jewish institution is 2 blocks, and the month is greater than 7. Otherwise, it takes the value 0.

    """
    df["jewish_inst_only_one_block_away"] = (
        df["jewish_inst_one_block_away"] - df["jewish_inst"]
    )
    df[f"{time_variable}_dummy"] = df[time_variable]
    df = pd.get_dummies(df, columns=[f"{time_variable}_dummy"], drop_first=False)
    df["post"] = np.where(df[time_variable] > event_time, 1, 0)
    df["treatment"] = df["jewish_inst"] * df["post"]
    df["treatment_1d"] = df["jewish_inst_only_one_block_away"] * df["post"]
    df["treatment_2d"] = np.where(df["distance_to_jewish_inst"] == 2, 1, 0) * df["post"]

    return df


def _neighborhood_numbering(df):
    """Assigns a unique integer to each neighborhood in the DataFrame `df`, based on its
    name.

    Args:
        df (pandas.DataFrame): A DataFrame containing the 'neighborhood' column.

    Returns:
        pandas.DataFrame: A copy of the input DataFrame, with an additional column named
        'n_neighborhood' that contains the integer code for each neighborhood.

    """
    df["n_neighborhood"] = 0
    df.loc[df["neighborhood"] == "Belgrano", "n_neighborhood"] = 1
    df.loc[df["neighborhood"] == "Once", "n_neighborhood"] = 2
    df.loc[df["neighborhood"] == "V. Crespo", "n_neighborhood"] = 3

    return df


def process_crime_by_block(df, maxrange=24):
    """Processes the crime data in the given DataFrame, `df`, and returns a panel data
    structure with information on theft and individual characteristics by block and
    month.

    Args:
        df (pandas.DataFrame): A DataFrame containing crime data.

    Returns:
        pandas.DataFrame: A panel data structure with information on theft and
        individual characteristics by block and month.

    """
    df = _clean_column_names_block(df)
    df = _convert_dtypes(df)

    theft_data = df.loc[:, df.columns.str.startswith("theft")]
    ind_char_data = df[[col for col in df.columns if not col.startswith("theft")]]

    for i in range(1, maxrange):
        theft_data.loc[theft_data[f"theft{i}corner"] == 1, f"theft{i}"] = 0.25

    for month in range(4, 13):
        theft_data = _split_theft_data(theft_data, month)
        theft_data = _calculate_total_theft_by_suffix(theft_data, month)
        theft_data = _calculate_theft_differences(theft_data, month)

    theft_cols = [col for col in theft_data.columns if col.startswith("theft")]
    theft_data = theft_data.drop(columns=theft_cols)
    theft_data = theft_data.reset_index(names=["block"])

    crime_by_block_panel = _create_panel_data(ind_char_data, theft_data)
    crime_by_block_panel = _create_new_variables(
        crime_by_block_panel,
        time_variable="month",
        event_time=7,
    )
    crime_by_block_panel = crime_by_block_panel.set_index(["block", "month"])

    return crime_by_block_panel


def process_ind_char_data(df):
    df = _clean_column_names_block(df)
    df = _convert_dtypes(df)

    ind_char_data = df[[col for col in df.columns if not col.startswith("theft")]]

    ind_char_data = _create_new_variables_ind(ind_char_data)
    ind_char_data = _drop_repated_obs(ind_char_data)

    return ind_char_data


def process_weekly_panel(df):
    """The process_weekly_panel function takes a pandas DataFrame as input, cleans and
    processes the data, and returns the processed DataFrame.

    Args:
    df (pandas.DataFrame): The input DataFrame containing the raw data.

    Returns:
    pandas.DataFrame: A processed DataFrame containing the following new columns:
    - Cleaned column names.
    - Converted data types.
    - New variables created based on the time variable and event time.
    - Neighborhood numbering.
    - A column that combines the week and block variables.
    - A column that calculates the average weekly thefts based on the total thefts.

    """
    df = _clean_column_names_weekly(df)
    df = df.convert_dtypes()
    df = _create_new_variables(df, time_variable="week", event_time=18)
    df = _neighborhood_numbering(df)
    df["neighborhood_week"] = 1 * df["week"] + 1000 * df["block"]
    df["av_weekly_thefts"] = df["total_thefts"] * ((365 / 12) / 7)

    return df
