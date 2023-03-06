"""Function(s) for cleaning the data set(s)."""
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

for key, value in month_dict.items():
    for i in range(1, 23):
        rob_data[f"rob{i}"] = rob_data[f"rob{i}"].astype(float)
        rob_data[f"rob{i}val"] = rob_data[f"rob{i}val"].astype(float)
        rob_data.loc[rob_data[f"rob{i}esq"] == 1, f"rob{i}"] = 0.25

        rob_data[f"ron{i}{value}"] = np.where(
            (rob_data[f"rob{i}"] != 0)
            & (rob_data[f"rob{i}mes"] == key)
            & (rob_data[f"rob{i}val"].between(8403.826, 100000)),
            rob_data[f"rob{i}"],
            0,
        )

        rob_data[f"rod{i}{value}"] = np.where(
            (rob_data[f"rob{i}"] != 0)
            & (rob_data[f"rob{i}mes"] == key)
            & (rob_data[f"rob{i}val"].between(0, 8403.826)),
            rob_data[f"rob{i}"],
            0,
        )

    rob_data[f"totrond{key}"] = rob_data.filter(regex=f"ron\\d+{value}").sum(axis=1)
    rob_data[f"totrobd{key}"] = rob_data.filter(regex=f"rod\\d+{value}").sum(axis=1)
    rob_data[f"difdn{key}"] = rob_data[f"totrond{key}"] - rob_data[f"totrobd{key}"]
