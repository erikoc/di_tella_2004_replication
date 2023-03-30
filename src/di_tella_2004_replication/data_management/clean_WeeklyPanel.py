"""Function(s) for cleaning the data set(s)."""
from di_tella_2004_replication.data_management.clean_crime_by_block import (
    _create_new_variables,
)


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
