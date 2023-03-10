"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd
import pyreadstat  as pyread



""" Monthly Panel """ 

### Reading the data ###

MonthlyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/MonthlyPanel.dta')

### Renaming columns ###

MonthlyPanel.columns = (
    MonthlyPanel.columns.str.replace("observ", "observ")
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
    .str.replace("mes", "month")
)

###### Data management ######

############################################## PART 1 ########################################################################################################################

# gen inst3_1=institu3-institu1;
MonthlyPanel['jewish_inst_one_block_away_1'] = MonthlyPanel['jewish_inst_one_block_away'] - MonthlyPanel['jewish_inst']
# Creating a list (using list comprehension) of the columns to be created
list_names_m = ["month5"]
#list_names.extend([list_names] + [f"month{i}" for i in range(6,13)])
list_names_m.extend([f"month{i}" for i in range(6,13)])
# Now using create twelve columns with the above names full of zeros
# gen month5=0; ... gen month12=0;
MonthlyPanel[[col for col in list_names_m]] = 0
# Now applying the conditions associated to them
# replace month5=1 if mes==5; ... replace month12=1 if mes==12;
for i in range(5,13):
    MonthlyPanel.loc[MonthlyPanel['month']==i, f"month{i}"]=1
# gen post=0;
MonthlyPanel['post'] = 0
# replace post=1 if mes>7;
MonthlyPanel.loc[MonthlyPanel['mes']>7, 'post']=1 # MonthlyPanel['post'].value_counts()[1], to count the number of 1s we have
# gen inst1p=institu1*post; # gen inst3_1p=inst3_1*post;
list1p_m = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
list1_m = ["jewish_inst",  "jewish_int_one_block_away_1"]
for colp, col in zip(list1p_m, list1_m):
        MonthlyPanel[colp] = MonthlyPanel[col]*MonthlyPanel['post']       
# gen cuad0=0; ... gen cuad7=0;
list_names2_m = ["cuad0"]
#list_names.extend([list_names] + [f"month{i}" for i in range(6,13)])
list_names2_m.extend([f"cuad{i}" for i in range(1,8)])
# Now using create twelve columns with the above names full of zeros
# gen cuad0=0; ... gen cuad7=0;
MonthlyPanel[[col for col in list_names2_m]] = 0
# Now applying the conditions associated to them
# replace cuad0=1 if distanci==0; ... replace cuad7=1 if distanci==7;
for i in range(0,8):
    MonthlyPanel.loc[MonthlyPanel['distance_to_jewish_inst']==i, f"cuad{i}"]=1
#MonthlyPanel[[col for col in list_names22]] = MonthlyPanel['cuad0']*MonthlyPanel['post']
for col in list_names2_m:
    MonthlyPanel[col+f'p'] = MonthlyPanel[col]*MonthlyPanel['post']  
# gen codigo=4;
MonthlyPanel['code'] = 4
# replace codigo=1 if institu1==1; replace codigo=2 if inst3_1==1; replace codigo=3 if cuad2==1;
list2_m = ['jewish_inst', 'jewish_int_one_block_away_1', 'cuad2']
for col, i in zip(list2_m, range(1,4)):
    MonthlyPanel.loc[MonthlyPanel[col]==1, 'code']=i
# gen otromes1=mes;
MonthlyPanel['othermonth1'] = MonthlyPanel['month']
# replace otromes1=7.2 if mes==72; replace otromes1=7.3 if mes==73;
for i,j in zip([72,73], [7.2,7.3]):
    MonthlyPanel.loc[MonthlyPanel['month']==i, 'othermonth1']=j
# sort observ mes;
MonthlyPanel = MonthlyPanel.sort_values(['observ', 'month'])
# by observ: gen totrob2=sum(totrob) if (mes==72 | mes==73);
MonthlyPanel = MonthlyPanel.assign(total_thefts2=pd.Series())
for i in range(1, len(MonthlyPanel)):
   if MonthlyPanel['month'].iloc[i] == 72 or MonthlyPanel['month'].iloc[i] == 73:
      MonthlyPanel['total_thefts2'].iloc[i] == MonthlyPanel['total_thefts'].iloc[i].cumsum()
# replace totrob2=totrob if (mes~=72 & mes~=73);
MonthlyPanel.loc[(MonthlyPanel['month'] != 72) & (MonthlyPanel['month'] != 73), 'total_thefts2'] = MonthlyPanel['total_thefts']

# Saving the data frame
MonthlyPanel.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel.csv')

############################################## PART 2 ########################################################################################################################

# drop if mes==72; drop if mes==73;
MonthlyPanel2 = MonthlyPanel.drop(MonthlyPanel.loc[(MonthlyPanel['month']==72) | (MonthlyPanel['month']==73)].index, inplace=True)

# Saving the data frame
MonthlyPanel2.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel2.csv')




""" Weekly Panel """

### Reading the data ###

WeeklyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/WeeklyPanel.dta')

### Renaming columns ###

WeeklyPanel.columns = (
    WeeklyPanel.columns.str.replace("observ", "observ")
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
    .str.replace("week", "week")
)

###### Data management ######

# Drop variables
WeeklyPanel.drop(columns=['street', 'street_nr', 'public_building_or_embassy', 'gas_station', 'bank'], inplace=True)

# Generate variables 

# gen week1=0; ... gen week39=0;
list_names = ["week1"]
list_names.extend([f"week{i}" for i in range(2,40)])
WeeklyPanel[[col for col in list_names]] = 0
# replace semana1=1 if week==1; ... replace semana39=1 if week==39;
for i in range(1,40):
    WeeklyPanel.loc[WeeklyPanel['week']==i, f"week{i}"]=1
# gen cuad2=0; # gen post=0; # gen n_neighborhood=0;
list1 = ["cuad2", "post", "n_neighborhood"]
WeeklyPanel[[col for col in list1]] = 0
# replace cuad2=1 if distance_to_jewish_inst==2;
WeeklyPanel.loc[WeeklyPanel['distance_to_jewish_inst']==2, 'cuad2']=1
# replace post=1 if week>=18;
WeeklyPanel.loc[WeeklyPanel['week']>18, 'post']=1
# gen jewish_inst_one_block_away_1=jewish_inst_one_block_away-jewish_inst;
WeeklyPanel['jewish_int_one_block_away_1'] = WeeklyPanel['jewish_inst_one_block_away'] - WeeklyPanel['jewish_inst']
# gen jewish_inst_p, jewish_int_one_block_away_1_p, cuad2p
list2p = ["jewish_inst_p", "jewish_int_one_block_away_1_p", "cuad2p"]
list2 = ["jewish_inst",  "jewish_int_one_block_away_1", "cuad2"]
for colp, col in zip(list2p, list2):
        WeeklyPanel[colp] = WeeklyPanel[col]*WeeklyPanel['post']
# replace n_neighborhood=1 if neighborhood=="Belgrano"; replace n_neighborhood=2 if neighborhood=="Once"; replace n_neighborhood=3 if neighborhood=="V. Crespo";      
list3 = ["Belgrano", "Once", "V. Crespo"]
for col, i in zip(list3, range(1,4)):
    WeeklyPanel.loc[WeeklyPanel['neighborhood']==col, 'n_neighborhood']=i
# gen codigo2=week+10000*n_neighborhood;
WeeklyPanel['codigo2'] = WeeklyPanel['week'] + 1000*WeeklyPanel['n_neighborhood']
# gen ntotrob=totrob*((365/12)/7);
WeeklyPanel['n_total_thefts'] = WeeklyPanel['total_thefts'] * (365/12)/7

# Saving the data frame
WeeklyPanel.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/WeeklyPanel.csv')







""" Crime by block """

# Reading the data

crime_by_block, meta = pyread.read_dta("..\\data\\CrimebyBlock.dta")

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
