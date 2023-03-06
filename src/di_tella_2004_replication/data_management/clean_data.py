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
