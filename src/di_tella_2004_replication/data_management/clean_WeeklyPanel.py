"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd
import pyreadstat  as pyread

""" Weekly Panel """

### Reading the data ###

WeeklyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/WeeklyPanel.dta')

### Renaming columns ###

def _clean_column_names_we(df):
    
    """This function takes a pandas DataFrame and standardizes the column names to a
    specified format.

    The function renames the columns by replacing certain substrings with standardized terms such as "rob" with "theft",
    "day" with "week_day", "dia" with "day", "mes" with "month", "hor" with "hour", "mak" with "brand", and "esq" with "corner".

    In addition, the function replaces specific column names with more meaningful and descriptive names, as specified in the
    'replacements' dictionary.

    Parameters:
    df (pandas.DataFrame): The pandas DataFrame containing the data to be standardized.

    Returns:
    pandas.DataFrame: The input DataFrame with the columns standardized to the specified format."""

    df.columns = (
        df.columns.str.replace("observ", "observ")
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
    
    return df

# _clean_column_names(df)
df_we = WeeklyPanel


###### Data management ######
"""""
# Drop variables
WeeklyPanel.drop(columns=['street', 'street_nr', 'public_building_or_embassy', 'gas_station', 'bank'], inplace=True)
"""""

def _drop_variables_we(df, list_drop):
    df.drop(columns=list_drop, inplace=True)
    return df

# def _drop_variables(df, list_drop)
list_drop_we = ['street', 'street_nr', 'public_building_or_embassy', 'gas_station', 'bank']

"""""
"fixed extension"
# gen week1=0; ... gen week39=0;
list_names = ["week1"]
list_names.extend([f"week{i}" for i in range(2,40)])
WeeklyPanel[[col for col in list_names]] = 0
# replace semana1=1 if week==1; ... replace semana39=1 if week==39;
for i in range(1,40):
    WeeklyPanel.loc[WeeklyPanel['week']==i, f"week{i}"]=1
"""""
    
def _gen_rep_variables_fixedextension_we(df,list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext):    
    """"This functions has certain inputs to generate a variable and replace its values (list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext)""" 
    for i in range_ext:
        list_names_ext.extend([f"week{i}"])  
    df[[col for col in list_names_ext]] = original_value_var # generate
    for i in range_loop:
        df.loc[df[var_cond_ext]==i, f"week{i}"]= final_value_var # replace
    return df

# _gen_rep_variables_we(df, type_of_list, list_names_ext, ext_cond, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext) # FIXED EXTENSION
list_names_ext_we = ["week1"]
range_ext_we = range(2,40)
original_value_var_we = 0
final_value_var_we = 1
range_loop_we = range(1,40)
var_cond_ext_we = 'week'

"""""
"fixed list simple"
# gen cuad2=0; # gen post=0; # gen n_neighborhood=0;
list1 = ["cuad2", "post", "n_neighborhood"]
WeeklyPanel[[col for col in list1]] = 0
# replace cuad2=1 if distance_to_jewish_inst==2;
WeeklyPanel.loc[WeeklyPanel['distance_to_jewish_inst']==2, 'cuad2']=1
"""""


def _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix): 
    """This functions has certain inputs to generate a variable and replace its values (list_fixed, var_cond_fix, var_fix, value_var_fix)"""
    df[[col for col in list_fixed]] = 0 # generate
    df.loc[df[var_cond_fix]==cond_fix, var_fix]= value_var_fix # replace
    return df
    
# _gen_rep_variables_fixed_list_simple_we(df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix)
list_fixed_we = ["cuad2", "post", "n_neighborhood"]
var_cond_fix_we = 'distance_to_jewish_inst'
cond_fix_we = 2
var_fix_we = 'cuad2'
value_var_fix_we = 1

"""""  
# replace post=1 if week>=18;
WeeklyPanel.loc[WeeklyPanel['week']>18, 'post']=1
"""""
        
def _rep_variables_we(df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace):
    
    """What this function does is just to replace a variable from a data frame depending on different types of conditions and with inputs 
    (var_cond_rep, condition_num, replace_var, value_replace)"""
    
    if type_of_condition == "bigger than":
        df.loc[df[var_cond_rep]>condition_num, replace_var]=value_replace
        return df
    elif type_of_condition == "smaller than":
        df.loc[df[var_cond_rep]<condition_num, replace_var]=value_replace
        return df
    elif type_of_condition == "equal to":
        df.loc[df[var_cond_rep]>condition_num, replace_var]=value_replace
        return df
    
# def _rep_variables(df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace)
type_of_condition_we = "bigger than"
var_cond_rep_we = 'week'
condition_num_we = 18
replace_var_we = 'post'
value_replace_we = 1

"""""
 # gen jewish_inst_one_block_away_1=jewish_inst_one_block_away-jewish_inst;
WeeklyPanel['jewish_int_one_block_away_1'] = WeeklyPanel['jewish_inst_one_block_away'] - WeeklyPanel['jewish_inst']
# gen codigo2=week+10000*n_neighborhood;
WeeklyPanel['code2'] = WeeklyPanel['week'] + 1000*WeeklyPanel['n_neighborhood']
# gen ntotrob=totrob*((365/12)/7);
WeeklyPanel['n_total_thefts'] = WeeklyPanel['total_thefts'] * (365/12)/7 

"""""
    
def _gen_diff_diff_variables_we(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_d, var1_d, var2_d, factor1_d, factor2_d)"""
    df[new_var_d] = (factor1_d*df[var1_d]) - (factor2_d*df[var2_d])
    return df

# _gen_diff_diff_variables(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d)
new_var_d_we = 'jewish_int_one_block_away_1'
var1_d_we = 'jewish_inst_one_block_away'
var2_d_we = 'jewish_inst'
factor1_d_we = 1
factor2_d_we = 1


def _gen_diff_sum_variables_we(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_s, var1_s, var2_s, factor1_s, factor2_s)"""
    df[new_var_s] = (factor1_s*df[var1_s]) + (factor2_s*df[var2_s])
    return df

# _gen_diff_sum_variables(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s)
new_var_s_we = 'code2'
var1_s_we = 'week'
var2_s_we = 'n_neighborhood'
factor1_s_we = 1
factor2_s_we = 1000

def _gen_simple_we(df, new_var_sim, var_sim, factor_sim):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_m, var1_m, var2_m, factor1_m, factor2_m)"""
    df[new_var_sim] = df[var_sim]*factor_sim
    return df

# _gen_simple_we(df, new_var_sim, var_sim, factor_sim)
new_var_sim_we = 'n_total_thefts'
var_sim_we = 'total_thefts'
factor_sim_we = (365/12)/7 

"""""
"fixed list complex"
# gen jewish_inst_p, jewish_int_one_block_away_1_p, cuad2p
list2p = ["jewish_inst_p", "jewish_int_one_block_away_1_p", "cuad2p"]
list2 = ["jewish_inst",  "jewish_int_one_block_away_1", "cuad2"]
for colp, col in zip(list2p, list2):
        WeeklyPanel[colp] = WeeklyPanel[col]*WeeklyPanel['post']
# replace n_neighborhood=1 if neighborhood=="Belgrano"; replace n_neighborhood=2 if neighborhood=="Once"; replace n_neighborhood=3 if neighborhood=="V. Crespo";      
list3 = ["Belgrano", "Once", "V. Crespo"]
for col, i in zip(list3, range(1,4)):
    WeeklyPanel.loc[WeeklyPanel['neighborhood']==col, 'n_neighborhood']=i
"""""


def _gen_rep_variables_fixedlistcomplex_we(df, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change): 
    """This functions has certain inputs to generate a variable and replace its values 
    (list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change)"""
    for col1, col2 in zip(list1_fix_com, list2_fix_com):
        df[col1] = df[col2]*df[var_fix_comp_mul]
    for col, i in zip(list_rep_fix_com, range_rep_fix_com):
        df.loc[df[var_fix_com_to_use]==col, var_fix_com_to_change]=i
    return df
    
# _gen_rep_variables_we(df, type_of_list, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change)
list1_fix_com_we = ["jewish_inst_p", "jewish_int_one_block_away_1_p", "cuad2p"]
list2_fix_com_we = ["jewish_inst",  "jewish_int_one_block_away_1", "cuad2"]
var_fix_comp_mul_we = 'post'
range_rep_fix_com_we = range(1,4)
list_rep_fix_com_we = ["Belgrano", "Once", "V. Crespo"]
var_fix_com_to_use_we = 'neighborhood'
var_fix_com_to_change_we = 'n_neighborhood'
  
"""""
# Saving the data frame
WeeklyPanel.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/WeeklyPanel.csv')
"""""

def _df_to_csv(df, location):
    "This functions saves a dataframe (df) as a csv file ina specific location (location)"
    df.to_csv(location)
    
# _df_to_csv(df, location)
location_we = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/WeeklyPanel.csv' 

#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def weeklypanel(df, 
                list_drop,
                list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext,
                list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix,
                type_of_condition, var_cond_rep, condition_num, replace_var, value_replace,
                new_var_d, var1_d, var2_d, factor1_d, factor2_d,
                new_var_s, var1_s, var2_s, factor1_s, factor2_s,
                new_var_sim, var_sim, factor_sim,
                list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change,
                location):
    df = _clean_column_names_we(df)
    df = _drop_variables_we(df, list_drop)
    df = _gen_rep_variables_fixedextension_we(df,list_names_ext, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext)
    df = _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix)
    df = _rep_variables_we(df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace)
    df = _gen_diff_diff_variables_we(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d)
    df = _gen_diff_sum_variables_we(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s)
    df = _gen_simple_we(df, new_var_sim, var_sim, factor_sim)
    df = _gen_rep_variables_fixedlistcomplex_we(df, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change)
    _df_to_csv(df, location)
    
WeeklyPanel = weeklypanel(df=WeeklyPanel,
                list_drop=list_drop_we,
                list_names_ext=list_names_ext_we, range_ext=range_ext_we, original_value_var=original_value_var_we, final_value_var=final_value_var_we, range_loop=range_loop_we, var_cond_ext=var_cond_ext_we,
                list_fixed=list_fixed_we, var_cond_fix=var_cond_fix_we, cond_fix=cond_fix_we, var_fix=var_fix_we, value_var_fix=value_var_fix_we,
                type_of_condition=type_of_condition_we, var_cond_rep=var_cond_rep_we, condition_num=condition_num_we, replace_var=replace_var_we, value_replace=value_replace_we,
                new_var_d=new_var_d_we, var1_d=var1_d_we, var2_d=var2_d_we, factor1_d=factor1_d_we, factor2_d=factor2_d_we,
                new_var_s=new_var_s_we, var1_s=var1_s_we, var2_s=var2_s_we, factor1_s=factor1_s_we, factor2_s=factor2_s_we,
                new_var_sim=new_var_sim_we, var_sim=var_sim_we, factor_sim=factor_sim_we,
                list1_fix_com=list1_fix_com_we, list2_fix_com=list2_fix_com_we, var_fix_comp_mul=var_fix_comp_mul_we, range_rep_fix_com=range_rep_fix_com_we, list_rep_fix_com=list_rep_fix_com_we, var_fix_com_to_use=var_fix_com_to_use_we, var_fix_com_to_change=var_fix_com_to_change_we,
                location=location_we)
