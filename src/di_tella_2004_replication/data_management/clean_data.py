"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd
import pyreadstat

# Reading the data

crime_by_block, meta = pyreadstat.read_dta("..\\data\\CrimebyBlock.dta")

# Fixing types

crime_by_block = crime_by_block.convert_dtypes()
crime_by_block = crime_by_block.set_index("observ")
float_cols = [f"rob{i}" for i in range(1, 23)] + [f"rob{i}val" for i in range(1, 23)]
crime_by_block[float_cols] = crime_by_block[float_cols].astype(float)


# Get columns starting with "ro"
rob_data = crime_by_block.loc[:, crime_by_block.columns.str.startswith("ro")]

# Get columns that don't start with "ro"
ind_char_data = crime_by_block[
    [col for col in crime_by_block.columns if not col.startswith("ro")]
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

# ron = high value; rod = low value

for key, value in month_dict.items():
    for i in range(1, 23):
        rob_data.loc[rob_data[f"rob{i}esq"] == 1, f"rob{i}"] = 0.25

        rob_data[f"theft_hv{i}{value}"] = np.where(
            (rob_data[f"rob{i}"] != 0)
            & (rob_data[f"rob{i}mes"] == key)
            & (rob_data[f"rob{i}val"].between(8403.826, 100000)),
            rob_data[f"rob{i}"],
            0,
        )

        rob_data[f"theft_lv{i}{value}"] = np.where(
            (rob_data[f"rob{i}"] != 0)
            & (rob_data[f"rob{i}mes"] == key)
            & (rob_data[f"rob{i}val"].between(0, 8403.826)),
            rob_data[f"rob{i}"],
            0,
        )

        rob_data[f"theft_night{i}{value}_2"] = np.where(
            (rob_data[f"rob{i}"] != 0)
            & (rob_data[f"rob{i}mes"] == key)
            & ((rob_data[f"rob{i}hor"] <= 10) | (rob_data[f"rob{i}hor"] > 22)),
            rob_data[f"rob{i}"],
            0,
        )

        rob_data[f"theft_day{i}{value}_2"] = np.where(
            (rob_data[f"rob{i}"] != 0)
            & (rob_data[f"rob{i}mes"] == key)
            & (rob_data[f"rob{i}hor"].between(10, 22, inclusive="right")),
            rob_data[f"rob{i}"],
            0,
        )

    rob_data[f"tot_theft_hv{key}"] = rob_data.filter(regex=f"theft_hv\\d+{value}").sum(
        axis=1,
    )
    rob_data[f"tot_theft_lv{key}"] = rob_data.filter(regex=f"theft_lv\\d+{value}").sum(
        axis=1,
    )
    rob_data[f"dif_hv_lv{key}"] = (
        rob_data[f"tot_theft_hv{key}"] - rob_data[f"tot_theft_lv{key}"]
    )

    rob_data[f"tot_theft_night{key}"] = rob_data.filter(
        regex=f"theft_night\\d+{value}",
    ).sum(axis=1)
    rob_data[f"tot_theft_day{key}"] = rob_data.filter(
        regex=f"theft_day\\d+{value}",
    ).sum(axis=1)
    rob_data[f"dif_night_day{key}"] = (
        rob_data[f"tot_theft_night{key}"] - rob_data[f"tot_theft_day{key}"]
    )


rob_cols = [col for col in rob_data.columns if col.startswith("ro")]
rob_data = rob_data.drop(columns=rob_cols)

rob_data = rob_data.reset_index()
rob_data = pd.wide_to_long(
    rob_data,
    stubnames=[
        "tot_theft_hv",
        "tot_theft_lv",
        "dif_hv_lv",
        "tot_theft_night",
        "tot_theft_day",
        "dif_night_day",
    ],
    i=["observ"],
    j="month",
)
