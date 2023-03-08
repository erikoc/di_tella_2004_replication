"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd
import pyreadstat

# Reading the data

crime_by_block, meta = pyreadstat.read_dta("..\\data\\CrimebyBlock.dta")

# Renaming columns

crime_by_block.columns = (
    crime_by_block.columns.str.replace("rob", "theft")
    .str.replace("day", "week_day")
    .str.replace("dia", "day")
    .str.replace("mes", "month")
    .str.replace("hor", "hour")
    .str.replace("mak", "brand")
    .str.replace("mak", "brand")
    .str.replace("mak", "brand")
    .str.replace("esq", "corner")
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

# Fixing types

crime_by_block = crime_by_block.convert_dtypes()
crime_by_block = crime_by_block.set_index("observ")
float_cols = [f"theft{i}" for i in range(1, 23)] + [
    f"theft{i}val" for i in range(1, 23)
]
crime_by_block[float_cols] = crime_by_block[float_cols].astype(float)


# Get columns starting with "ro"
theft_data = crime_by_block.loc[:, crime_by_block.columns.str.startswith("theft")]

# Get columns that don't start with "ro"
ind_char_data = crime_by_block[
    [col for col in crime_by_block.columns if not col.startswith("theft")]
]


# Getting variables of interest

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

# ideas:
#  1. do a map on a function to make this double loop more efficient
#  2. specify this condition np.where((theft_data[f"theft{i}"] != 0) & (theft_data[f"theft{i}mes"] == key)
#      above and call it onto the later code


for key, value in month_dict.items():
    for i in range(1, 23):
        theft_data.loc[theft_data[f"theft{i}corner"] == 1, f"theft{i}"] = 0.25

        theft_data[f"theft_hv{i}{value}"] = np.where(
            (theft_data[f"theft{i}"] != 0)
            & (theft_data[f"theft{i}month"] == key)
            & (theft_data[f"theft{i}val"].between(8403.826, 100000)),
            theft_data[f"theft{i}"],
            0,
        )

        theft_data[f"theft_lv{i}{value}"] = np.where(
            (theft_data[f"theft{i}"] != 0)
            & (theft_data[f"theft{i}month"] == key)
            & (theft_data[f"theft{i}val"].between(0, 8403.826)),
            theft_data[f"theft{i}"],
            0,
        )

        theft_data[f"theft_night{i}{value}"] = np.where(
            (theft_data[f"theft{i}"] != 0)
            & (theft_data[f"theft{i}month"] == key)
            & (
                (theft_data[f"theft{i}hour"] <= 10) | (theft_data[f"theft{i}hour"] > 22)
            ),
            theft_data[f"theft{i}"],
            0,
        )

        theft_data[f"theft_day{i}{value}"] = np.where(
            (theft_data[f"theft{i}"] != 0)
            & (theft_data[f"theft{i}month"] == key)
            & (theft_data[f"theft{i}hour"].between(10, 22, inclusive="right")),
            theft_data[f"theft{i}"],
            0,
        )

        theft_data[f"theft_weekday{i}{value}"] = np.where(
            (theft_data[f"theft{i}"] != 0)
            & (theft_data[f"theft{i}month"] == key)
            & (theft_data[f"theft{i}day"] <= 5),
            theft_data[f"theft{i}"],
            0,
        )

        theft_data[f"theft_weekend{i}{value}"] = np.where(
            (theft_data[f"theft{i}"] != 0)
            & (theft_data[f"theft{i}month"] == key)
            & (theft_data[f"theft{i}day"] > 5),
            theft_data[f"theft{i}"],
            0,
        )

    theft_data[f"tot_theft_hv{key}"] = theft_data.filter(
        regex=f"theft_hv\\d+{value}",
    ).sum(
        axis=1,
    )
    theft_data[f"tot_theft_lv{key}"] = theft_data.filter(
        regex=f"theft_lv\\d+{value}",
    ).sum(
        axis=1,
    )
    theft_data[f"dif_hv_lv{key}"] = (
        theft_data[f"tot_theft_hv{key}"] - theft_data[f"tot_theft_lv{key}"]
    )

    theft_data[f"tot_theft_night{key}"] = theft_data.filter(
        regex=f"theft_night\\d+{value}",
    ).sum(axis=1)
    theft_data[f"tot_theft_day{key}"] = theft_data.filter(
        regex=f"theft_day\\d+{value}",
    ).sum(axis=1)
    theft_data[f"dif_night_day{key}"] = (
        theft_data[f"tot_theft_night{key}"] - theft_data[f"tot_theft_day{key}"]
    )

    theft_data[f"tot_theft_weekday{key}"] = theft_data.filter(
        regex=f"theft_weekday\\d+{value}",
    ).sum(axis=1)
    theft_data[f"tot_theft_weekend{key}"] = theft_data.filter(
        regex=f"theft_weekend\\d+{value}",
    ).sum(axis=1)
    theft_data[f"dif_weekday_weekend{key}"] = (
        theft_data[f"tot_theft_weekday{key}"] - theft_data[f"tot_theft_weekend{key}"]
    )


theft_cols = [col for col in theft_data.columns if col.startswith("theft")]
theft_data = theft_data.drop(columns=theft_cols)

theft_data = theft_data.reset_index()
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
    i=["observ"],
    j="month",
)
