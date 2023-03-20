"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd
import pyreadstat  as pyread



""" Monthly Panel """ 

### Reading the data ###

MonthlyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/MonthlyPanel.dta')

### Renaming columns ###

def _clean_column_names_mon(df):
    
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


    df = (
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
        .str.replace("mes", "month")
    )
    
    return df

# _clean_column_names_mon(df)
df_m1 = MonthlyPanel

###### Data management ######

############################################## PART 1 ########################################################################################################################

"""""
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
"""""

def _gen_rep_var_fixed_extension_mon(df, range_ext, list_names_ext, ext_cond, original_value_var, range_loop, var_cond_ext, final_value_var):
    
    """This function is just generating new variables (columns) for our dataframe (df) given a certain condition. In this case, we generate variable based on a
    list extension (list_names_ext) which has a range of extension(range_ext). The extension of the list follows a condition of extension (ext_cond). In the end
    we give a value to all these variables (original_value_var). After that we replace the values of the variable given the condition that another variable
    (var_cond_ext) in the data frame has a certain value "i" which is part of a loop range (range_loop). The columns to be replaced are also the same as the 
    extension condition (ext_cond) that we used to extend the list to generate the variables. In the end, all the columns have a fixed replaced variable
    (final_value_var)"""
    
    for i in range_ext:
        list_names_ext.extend([ext_cond])  
    df[[col for col in list_names_ext]] = original_value_var # generate
    for i in range_loop:
        df.loc[df[var_cond_ext]==i, ext_cond]= final_value_var # replace
    return df

# _gen_rep_var_fixed_extension_mon(df, range_ext, list_names_ext, ext_cond, original_value_var, range_loop, var_cond_ext, final_value_var)
range_ext_m1 = range(6,13)
list_names_ext_m1 = ["month5"]
ext_cond_m1 = f"month{i}"
original_value_var_m1 = 0
range_loop_m1 = range(5,13)
var_cond_ext_m1 = 'month'
final_value_var_m1 = 1

"""""
# gen inst3_1=institu3-institu1;
MonthlyPanel['jewish_inst_one_block_away_1'] = MonthlyPanel['jewish_inst_one_block_away'] - MonthlyPanel['jewish_inst']
"""""

def _gen_var_difference_mon(df, new_var, var1, var_sub):
    
    """This function generates a new variable (new_var) in a dataframe (df) using existing columns of the dataframe (var1, var_sub) and substracting them.
    var_sub is the column being substracted"""
    df[new_var] = df[var1] - df[var_sub]
    return df

# _gen_var_difference_mon(df, new_var, var1, var_sub)
new_var_m1 = 'jewish_inst_one_block_away_1'
var1_m1 = 'jewish_inst_one_block_away'
var_sub_m1 = 'jewish_inst'
    
"""""
# gen post=0;
MonthlyPanel['post'] = 0
# replace post=1 if mes>7;
MonthlyPanel.loc[MonthlyPanel['month']>7, 'post']=1 # MonthlyPanel['post'].value_counts()[1], to count the number of 1s we have
"""""

def _gen_rep_var_single_cond_biggerthan_mon(df, var_gen, original_value, cond_var, final_value, cond):
    
    """This function tries to generate a variable (var_gen) in a data frame (df) with an original (original_value) value to later on replace it 
    given a condition of another column (cond_var) given it a specific value (final_value) if the bigger than condition (cond) is met"""
    
    df[var_gen] = original_value
    df.loc[df[cond_var]>cond, var_gen]= final_value
    return df

# _gen_rep_var_single_cond_biggerthan_mon(df, var_gen, original_value, cond_var, final_value, cond)
var_gen_m1 = 'post'
original_value_m1 = 0
cond_var_m1 = 'month'
final_value_m1 = 1
cond_m1 = 7
    

"""""    
# gen inst1p=institu1*post; # gen inst3_1p=inst3_1*post;
list1p_m = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
list1_m = ["jewish_inst",  "jewish_int_one_block_away_1"]
for colp, col in zip(list1p_m, list1_m):
        MonthlyPanel[colp] = MonthlyPanel[col]*MonthlyPanel['post'] 
""""" 

def _gen_var_double_listed_mon(df, list_gen_var, list_ori_var, fixed_var):
    
    """This function is generating variables listed on a list (list_gen_var) given a list of existing variables (list_ori_var) within a dataframe (df) 
    using a multiplication rule multiplying it by a fixed variable (fixed_var) already existent in thedataframe(df) """
    for col1, col2 in zip(list_gen_var, list_ori_var):
        df[col1] = df[col2]*df[fixed_var] 
    return df

# _gen_var_double_listed_mon(df, list_gen_var, list_ori_var, fixed_var)
list_gen_var_m1 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
list_ori_var_m1 = ["jewish_inst",  "jewish_int_one_block_away_1"]
fixed_var_m1 = 'post'
    
"""""            
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
    
"""""

# FOR THE ABOVE ONE USE _gen_rep_var_fixed_estension_mon()

def _gen_rep_var_fixed_extension_mon2(df, range_ext2, list_names_ext2, ext_cond2, original_value_var2, range_loop2, var_cond_ext2, final_value_var2):
    
    """This function is just generating new variables (columns) for our dataframe (df) given a certain condition. In this case, we generate variable based on a
    list extension (list_names_ext) which has a range of extension(range_ext). The extension of the list follows a condition of extension (ext_cond). In the end
    we give a value to all these variables (original_value_var). After that we replace the values of the variable given the condition that another variable
    (var_cond_ext) in the data frame has a certain value "i" which is part of a loop range (range_loop). The columns to be replaced are also the same as the 
    extension condition (ext_cond) that we used to extend the list to generate the variables. In the end, all the columns have a fixed replaced variable
    (final_value_var)"""
    
    for i in range_ext2:
        list_names_ext2.extend([ext_cond2])  
    df[[col for col in list_names_ext2]] = original_value_var2 # generate
    for i in range_loop2:
        df.loc[df[var_cond_ext2]==i, ext_cond2]= final_value_var2 # replace
    return df

# _gen_rep_var_fixed_extension_mon2(df, range_ext2, list_names_ext2, ext_cond2, original_value_var2, range_loop2, var_cond_ext2, final_value_var2)
range_ext2_m1 = range(1,8)
list_names_ext2_m1 = ["cuad0"]
ext_cond2_m1 = f"month{i}"
original_value_var2_m1 = 0
range_loop2_m1 = range(0,8)
var_cond_ext2_m1 = 'distance_to_jewish_inst'
final_value_var2_m1 = 1


"""""
#MonthlyPanel[[col for col in list_names22]] = MonthlyPanel['cuad0']*MonthlyPanel['post']
for col in list_names2_m:
    MonthlyPanel[col+f'p'] = MonthlyPanel[col]*MonthlyPanel['post'] 
""""" 

def _gen_var_cond_list_similar_mon(df, ori_variables, fixed_variable, name_change):
    
    """"This functions is trying to generate a list of variables that have a quite similar name compared to an already existing
    set of variables (ri_variables) in a dataframe (df) and it is generated by giving them a similar name with a new added condition to the original ones
    (name_change) and by multiplying the original columns of the df on a loop by a fixed column (fixed_variable) in the dataframe"""
    
    for col in ori_variables:
        df[col+name_change] = df[col]*df[fixed_variable]
    return df

# _gen_var_cond_list_similar_mon(df, ori_variables, fixed_variable, name_change)
ori_variables_m1 = ["cuad0", "cuad1", "cuad2", "cuad3", "cuad4", "cuad5", "cuad6", "cuad7"]
fixed_variable_m1 = "post"
name_change_m1 = f'p'
 
"""""   
# gen codigo=4;
MonthlyPanel['code'] = 4
# replace codigo=1 if institu1==1; replace codigo=2 if inst3_1==1; replace codigo=3 if cuad2==1;
list2_m = ['jewish_inst', 'jewish_int_one_block_away_1', 'cuad2']
for col, i in zip(list2_m, range(1,4)):
    MonthlyPanel.loc[MonthlyPanel[col]==1, 'code']=i
"""""

def _gen_rep_var_various_cond_equality_mon(df, new_gen_variable, new_original_value, list_ext_variables, range_new_gen, value_originallist):
    """This function is generating a new variable (new_gen_variable) in a dataframe (df) with an original value (new_original_value). 
    The values are replace with different values (range_new_gen) on a loop for the orginal list of variables (list_ext_variables, range_new_gen)
    depending on different conditions on other variables (value_originallist) already existing in the data frame"""
    df[new_gen_variable] = new_original_value # generate
    for col, i in zip(list_ext_variables, range_new_gen):
         df.loc[df[col]==value_originallist, new_gen_variable]=i # replace
    return df

# _gen_rep_var_various_cond_equality_mon(df, new_gen_variable, new_original_value, list_ext_variables, range_new_gen, value_originallist)
new_gen_variable_m1 = 'code'
new_original_value_m1 = 4
list_ext_variables_m1 = ['jewish_inst', 'jewish_int_one_block_away_1', 'cuad2']
range_new_gen_m1 = range(1,4)
value_originallist_m1 = 1

    
"""""
# gen otromes1=mes;
MonthlyPanel['othermonth1'] = MonthlyPanel['month']
# replace otromes1=7.2 if mes==72; replace otromes1=7.3 if mes==73;
for i,j in zip([72,73], [7.2,7.3]):
    MonthlyPanel.loc[MonthlyPanel['month']==i, 'othermonth1']=j
"""""

def _gen_rep_var_various_cond_equality_listedvalues_mon(df, NEW_var, ORI_var, list_a, list_b):
    """This function is generating a new variable (NEW_var) with an orginal value (ORI_var) and then replacing it with given a condition on an
    existent variable (list_a) on a dataframe and using elements of a list to replace the value (list_b)"""
    df[NEW_var] = df[ORI_var]
    for i,j in zip(list_a, list_b):
        df.loc[df[ORI_var]==i, NEW_var]=j
    return df

# _gen_rep_var_various_cond_equality_listedvalues_mon(df, NEW_var, ORI_var, list_a, list_b)
NEW_var_m1 = 'othermonth1'
ORI_var_m1 = 'month'
list_a_m1 = [72,73]
list_b_m1 = [7.2,7.3]
    
    
"""""
# sort observ mes;
MonthlyPanel = MonthlyPanel.sort_values(['observ', 'month'])
"""""

def _sort_mon(df, list_sort):
    """This funtions is sorting the values of a dataframe (df) given a list of values to be used for sorting (list_sort)"""
    df = df.sort_values(list_sort)
    return df

# _sort_mon(df, list_sort)
list_sort_m1 = ['observ', 'month']

"""""
# by observ: gen totrob2=sum(totrob) if (mes==72 | mes==73);
MonthlyPanel = MonthlyPanel.assign(total_thefts2=pd.Series())
for i in range(1, len(MonthlyPanel)):
   if MonthlyPanel['month'].iloc[i] == 72 or MonthlyPanel['month'].iloc[i] == 73:
      MonthlyPanel['total_thefts2'].iloc[i] == MonthlyPanel['total_thefts'].iloc[i].cumsum()
# replace totrob2=totrob if (mes~=72 & mes~=73);
MonthlyPanel.loc[(MonthlyPanel['month'] != 72) & (MonthlyPanel['month'] != 73), 'total_thefts2'] = MonthlyPanel['total_thefts']
"""""

def _gen_rep_total_thefts2_mon(df, var_complex_cond, cond1, cond2):
    """This funtion is generating a a new variable by observation "total_thefts2" in a dataframe (df) """
    df = df.assign(total_thefts2=pd.Series())
    for i in range(1, len(df)):  #### I am here 
        if df[var_complex_cond].iloc[i] == cond1 or df[var_complex_cond].iloc[i] == cond2:
            df['total_thefts2'].iloc[i] == df['total_thefts'].iloc[i].cumsum() # generate
    df[(df[var_complex_cond] != cond1) & (df[var_complex_cond] != cond2), 'total_thefts2'] = df['total_thefts'] # replace
    return df

# _gen_rep_total_thefts2(df, var_complex_cond, cond1, cond2)
var_complex_cond_m1 = 'month'
cond1_m1 = 72
cond2_m1 = 73
    

"""""
# Saving the data frame
MonthlyPanel.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel.csv')
"""""

def _df_to_csv_mon(df, location):
    "This functions saves a dataframe (df) as a csv file ina specific location (location)"
    df.to_csv(location)

# _df_to_csv_mon(df, location)
location_m1 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel.csv'

    

#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_1(df, 
                   range_ext, list_names_ext, ext_cond, original_value_var, range_loop, var_cond_ext, final_value_var,
                   new_var, var1, var_sub,
                   var_gen, original_value, cond_var, final_value, cond,
                   list_gen_var, list_ori_var, fixed_var,
                   range_ext2, list_names_ext2, ext_cond2, original_value_var2, range_loop2, var_cond_ext2, final_value_var2,
                   ori_variables, fixed_variable, name_change,
                   new_gen_variable, new_original_value, list_ext_variables, range_new_gen, value_originallist,
                   NEW_var, ORI_var, list_a, list_b,
                   list_sort,
                   var_complex_cond, cond1, cond2,
                   location):
    df = _clean_column_names_mon(df)
    df = _gen_rep_var_fixed_extension_mon(df, range_ext, list_names_ext, ext_cond, original_value_var, range_loop, var_cond_ext, final_value_var)
    df = _gen_var_difference_mon(df, new_var, var1, var_sub)
    df = _gen_rep_var_single_cond_biggerthan_mon(df, var_gen, original_value, cond_var, final_value, cond)
    df = _gen_var_double_listed_mon(df, list_gen_var, list_ori_var, fixed_var)
    df = _gen_rep_var_fixed_extension_mon2(df, range_ext2, list_names_ext2, ext_cond2, original_value_var2, range_loop2, var_cond_ext2, final_value_var2)
    df = _gen_var_cond_list_similar_mon(df, ori_variables, fixed_variable, name_change)
    df = _gen_rep_var_various_cond_equality_mon(df, new_gen_variable, new_original_value, list_ext_variables, range_new_gen, value_originallist)
    df = _gen_rep_var_various_cond_equality_listedvalues_mon(df, NEW_var, ORI_var, list_a, list_b)
    df = _sort_mon(df, list_sort)
    df = _gen_rep_total_thefts2_mon(df, var_complex_cond, cond1, cond2)
    _df_to_csv_mon(df, location)


### FUNCTION ###

MonthlyPanel = monthlypanel_1(df=df_m1, 
                              range_ext=range_ext_m1, list_names_ext=list_names_ext_m1, ext_cond=ext_cond_m1, original_value_var=original_value_var_m1, range_loop=range_loop_m1, var_cond_ext=var_cond_ext_m1, final_value_var=final_value_var_m1,
                              new_var=new_var_m1, var1=var1_m1, var_sub=var_sub_m1,
                              var_gen=var_gen_m1, original_value=original_value_m1, cond_var=cond_var_m1, final_value=final_value_m1, cond=cond_m1,
                              list_gen_var=list_gen_var_m1, list_ori_var=list_ori_var_m1, fixed_var=fixed_var_m1,
                              range_ext2=range_ext2_m1, list_names_ext=list_names_ext2_m1, ext_cond=ext_cond2_m1, original_value_var=original_value_var2_m1, range_loop=range_loop2_m1, var_cond_ext=var_cond_ext2_m1, final_value_var=final_value_var2_m1,
                              ori_variables=ori_variables_m1, fixed_variable=fixed_variable_m1, name_change=name_change_m1,
                              new_gen_variable=new_gen_variable_m1, new_original_value=new_original_value_m1, list_ext_variables=list_ext_variables_m1, range_new_gen=range_new_gen_m1, value_originallist=value_originallist_m1,
                              NEW_var=NEW_var_m1, ORI_var=ORI_var_m1, list_a=list_a_m1, list_b=list_b_m1,
                              list_sort=list_sort_m1, var_complex_cond=var_complex_cond_m1, cond1=cond1_m1, cond2=cond2_m1,
                              location=location_m1)

   

############################################## PART 2 ########################################################################################################################
"""""
### Reading the data ###
MonthlyPanel = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel.csv')
"""""
def _reading_data_csv(location_origin):
    df = pd.read_csv(location_origin)
    return df

# _reading_data_csv(location_origin)
location_origin_m2 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel.csv'
    
"""""
# drop if mes==72; drop if mes==73;
MonthlyPanel2 = MonthlyPanel.drop(MonthlyPanel.loc[(MonthlyPanel['month']==72) | (MonthlyPanel['month']==73)].index, inplace=True)
"""""

def _drop_if_simple_mon2(df, var_drop, drop1, drop2):    
    """This function is dropping a variable (var_drop) from a dataframe (df) based on two condition (drop1, drop2)"""    
    df.drop(df.loc[(df[var_drop]==drop1) | (df[var_drop]==drop2)].index, inplace=True)
    return df

# _drop_if_simple_m2(df, var_drop, drop1, drop2):
df_m2 = MonthlyPanel
var_drop_m2 = 'month'
drop1_m2 = 72
drop2_m2 = 73


"""""   
# gen totrobc=totrob;
MonthlyPanel2['total_thefts_c'] = MonthlyPanel2['total_thefts']
# rep totrobc=totrob*(30/17) if mes==7; and if mes==5, 8, 10 or 12 then replace totrobc=totrob*(30/31)
MonthlyPanel2.loc[MonthlyPanel2['month']==7, "total_thefts_c"]= MonthlyPanel2['total_thefts']*(30/17)
MonthlyPanel2.loc[(MonthlyPanel2['month']==5) | (MonthlyPanel2['month']==8) | (MonthlyPanel2['month']==10) | (MonthlyPanel2['month']==12), "total_thefts_c"]= MonthlyPanel2['total_thefts']*(30/31)
"""""

def _gen_rep_various_cond_mon2(df, new_var_v_cond, ori_var_v_cond, var_con_v_cond, con_v_cond1, con_v_cond2, con_v_cond3, con_v_cond4, con_v_cond5, multiple1_v_cond, multiple2_v_cond):
    """This function is creating a new variable (new_var_v_cond) in a dataframe (df), based on the values of another existing variable ori_var_v_cond
    To replace the original values assigned to this new variable, we a condition on a variable in the dataframe (var_con_v_cond) is used
    and there are different possible values for this variable (con_v_cond1, con_v_cond2, con_v_cond3, con_v_cond4, con_v_cond5) and that changes the condition 
    and depending on the conditions, the variables get a new value scaled by different factors (multiple1_v_cond, multiple2_v_cond) 
    multiplied by the original variable (ori_var_v_cond)""" 
    df[new_var_v_cond] = df[ori_var_v_cond] # generate
    df.loc[df[var_con_v_cond]==con_v_cond1, new_var_v_cond]= MonthlyPanel2[ori_var_v_cond]*multiple1_v_cond # replace
    MonthlyPanel2.loc[(MonthlyPanel2[var_con_v_cond]==con_v_cond2) | (MonthlyPanel2[var_con_v_cond]==con_v_cond3) | (MonthlyPanel2[var_con_v_cond]==con_v_cond4) | (MonthlyPanel2[var_con_v_cond]==con_v_cond5), new_var_v_cond]= MonthlyPanel2[ori_var_v_cond]*multiple2_v_cond # replace
    return df 

# _gen_rep_various_cond_m2(df, new_var_v_cond, ori_var_v_cond, var_con_v_cond, con_v_cond1, con_v_cond2, con_v_cond3, con_v_cond4, con_v_cond5, multiple1_v_cond, multiple2_v_cond)
new_var_v_cond_m2 = 'total_thefts_c'
ori_var_v_cond_m2 = 'total_thefts'
var_con_v_cond_m2 = 'month'
con_v_cond1_m2 = 7
con_v_cond2_m2 = 5
con_v_cond3_m2 = 8
con_v_cond4_m2 = 10
con_v_cond5_m2 = 12
multiple1_v_cond_m2 = (30/17)
multiple2_v_cond_m2 = (30/31)

"""""
# gen prerob=.; gen posrob=.; gen robcoll=.;
list_namess = ['prethefts', 'posthefts', 'theftscoll']
for col in list_namess:
    MonthlyPanel2[col] = pd.NA  
# replace prerob=totrob if mes<8;
MonthlyPanel2.loc[MonthlyPanel2['month']<8, 'prethefts']=MonthlyPanel2['total_thefts']
# replace posrob=totrob if mes>7;
MonthlyPanel2.loc[MonthlyPanel2['month']>7, 'posthefts']=MonthlyPanel2['total_thefts']
"""""

def _genNA_rep_two_cond_mon2(df, list_for_NA, var_con_NA, fixed_var_NA, NA_value):
    """This generates a set of NA variables in a dataframe(df) based on a list of variables (list_for_NA). Then the value of some of the variables
    on the list are replaced with values of an original variable in the dataframe (fixed_var_NA) and this is based on a list on conditions over a variable (var_con_NA)
    checking whether this variable has a value higher or lower than a fixed value (NA_value)
    """    
    for col in list_for_NA:
        df[col] = pd.NA  # generate
    df.loc[df[var_con_NA]<NA_value+1, list_for_NA[0]]=df[fixed_var_NA]
    df.loc[df[var_con_NA]>NA_value, list_for_NA[1]]=df[fixed_var_NA]
    return df

# _genNA_rep_two_cond_m2(df, list_for_NA, var_con_NA, fixed_var_NA, NA_value)
list_for_NA_m2 = ['prethefts', 'posthefts', 'theftscoll']
var_con_NA_m2 = 'month'
fixed_var_NA_m2 = 'total_thefts'
NA_value_m2 = 7
    
"""""
# sort observ mes;
MonthlyPanel2 = MonthlyPanel2.sort_values(['observ', 'month'])
"""""

### USING THIS FROM ABOVE MADE DATA FRAME
def _sort_mon2(df, list_sort):
    """This funtions is sorting the values of a dataframe (df) given a list of values to be used for sorting (list_sort)"""
    df = df.sort_values(list_sort)
    return df

list_sort_m2 = ['observ', 'month']

"""""
# egen totpre=sum(prerob), by (observ);
MonthlyPanel2['totalpre'] = MonthlyPanel2.groupby('observ')['prethefts'].transform('sum') # The transform() function is used to apply the calculation to each row of the DataFrame, 
# rather than aggregating the data by group. This produces a new column with the same length as the original DataFrame.
# replace robcoll=totpre/4 if mes==4;
MonthlyPanel2.loc[MonthlyPanel2['month']==4, 'theftscoll']=MonthlyPanel2['totalpre']/4
"""""

def _egen_rep_mon2(df, new_egen_var, by_var, var_egen_sup, cond_ege_var, cond_ege_val, ege_var_change, ege_scale_factor):
    """This function is generating a new variable (new_egen_var) based on a by variable (by_var) and taking the sum of an already
    existing variable (var_egen_sup) in a dataframe (df). Later, an alreadz existing variable (ege_var_change) in the dataframe is
    replaced by the new variable generated in this function (new_egen_var) scaled by a given number (ege_scale_factor)
    and this is given the value of an already existing variable in a the dataframe (cond_ege_var) meeting a certain value (cond_ege_val)"""
    df[new_egen_var] = df.groupby(by_var)[var_egen_sup].transform('sum')
    df.loc[df[cond_ege_var]==cond_ege_val, ege_var_change]=df[new_egen_var]/ege_scale_factor
    return df

# _egen_rep_mon2(df, new_egen_var, by_var, var_egen_sup, cond_ege_var, cond_ege_val, ege_var_change, ege_scale_factor)
new_egen_var_m2 = 'totalpre'
by_var_m2 = 'observ' 
var_egen_sup_m2 = 'prethefts'
cond_ege_var_m2 = 'month'
cond_ege_val_m2 = 4
ege_var_change_m2 = 'theftscoll'
ege_scale_factor_m2 = 4

"""""
# egen totpos=sum(posrob), by (observ);
MonthlyPanel2['totalpos'] = MonthlyPanel2.groupby('observ')['posthefts'].transform('sum')
# replace robcoll=totpos/5 if mes==8;
MonthlyPanel2.loc[MonthlyPanel2['month']==8, 'theftscoll']=MonthlyPanel2['totalpre']/5
"""""

def _egen_rep2_mon2(df, new_egen_var2, by_var2, var_egen_sup2, cond_ege_var2, cond_ege_val2, ege_var_change2, ege_scale_factor2):
    """This function is generating a new variable (new_egen_var) based on a by variable (by_var) and taking the sum of an already
    existing variable (var_egen_sup) in a dataframe (df). Later, an alreadz existing variable (ege_var_change) in the dataframe is
    replaced by the new variable generated in this function (new_egen_var) scaled by a given number (ege_scale_factor)
    and this is given the value of an already existing variable in a the dataframe (cond_ege_var) meeting a certain value (cond_ege_val)"""
    df[new_egen_var2] = df.groupby(by_var2)[var_egen_sup2].transform('sum')
    df.loc[df[cond_ege_var2]==cond_ege_val2, ege_var_change2]=df[new_egen_var2]/ege_scale_factor2
    return df

# _egen_rep_mon2(df, new_egen_var, by_var, var_egen_sup, cond_ege_var, cond_ege_val, ege_var_change, ege_scale_factor)
new_egen_var2_m2 = 'totalpos'
by_var2_m2 = 'observ' 
var_egen_sup2_m2 = 'posthefts'
cond_ege_var2_m2 = 'month'
cond_ege_val2_m2 = 8
ege_var_change2_m2 = 'theftscoll'
ege_scale_factor2_m2 = 5

"""""
# gen totrobq=totrob*4;
MonthlyPanel2['total_thefts_q'] = MonthlyPanel2['total_thefts'] * 4
"""""
def _gen_simplebased_mon2(df, new_generated_var, existing_var, scalar_gen):
    """This function generates a variable (new_generated_var) in a dataframe (df) given another existing variable
    in the dataframe (existing_var) and a scalar multiplication """
    df[new_generated_var] = df[existing_var] * scalar_gen
    return df
    
# _gen_simplebased_mon2(df, new_generated_var, existing_var, scalar_gen):
new_generated_var_m2 = 'total_thefts_q' 
existing_var_m2 = 'total_thefts'
scalar_gen_m2 = 4

"""""   
# gen w=0.25;
MonthlyPanel2['w'] = 0.25
"""""

def _gen_simple_mon2(df, new_gen_var_sim, value_sim):
    """This function generates a variable (new_generated_var) in a dataframe (df) giving it a certain scalar value"""
    df[new_gen_var_sim] = value_sim
    return df

# _gen_simple_mon2(df, new_gen_var_sim, value_sim)
new_gen_var_sim_m2 = 'w'  
value_sim_m2 = 0.25

"""""
# gen nbarrio=0;
MonthlyPanel2['n_neighborhood'] = 0
"""""

def _gen_simple2_mon2(df, new_gen_var_sim2, value_sim2):
    """This function generates a variable (new_generated_var) in a dataframe (df) giving it a certain scalar value"""
    df[new_gen_var_sim2] = value_sim2
    return df

# _gen_simple_mon2(df, new_gen_var_sim, value_sim)
new_gen_var_sim2_m2 = 'n_neighborhood'  
value_sim2_m2 = 0


"""""
# replace nbarrio=1 if barrio=="Belgrano"; replace nbarrio=2 if barrio=="Once"; replace nbarrio=3 if barrio=="V. Crespo";
list_names_n = ['Belgrano', 'Once', 'V. Crespo']
for col, i in zip(list_names_n, range(1,4)):
    MonthlyPanel2.loc[MonthlyPanel2['neighborhood']==col, 'n_neighborhood']=i 

# gen codigo2=mes+10000*nbarrio;
MonthlyPanel2['code2'] = MonthlyPanel2['month'] + 1000*MonthlyPanel2['n_neighborhood']

# gen belgrano=0; # gen once=0; # gen vcrespo=0; # gen month4=0;
# replace belgrano=1 if barrio=="Belgrano"; replace once=1 if barrio=="Once"; replace vcrespo=1 if barrio=="V. Crespo";
list_names_b = ['belgrano', 'once', 'vcrespo']
for col1, col2 in zip(list_names_b, list_names_n):
    MonthlyPanel2[col1] = 0
    MonthlyPanel2.loc[MonthlyPanel2['neighborhood']==col2, col1]=1
"""""

def _complex_gen_rep_mon2(df, list_names_complex, range_complex, var_rep_cond_complex, var_rep_complex, gen_var_complex, var_cond_complex, scale_complex, list_names_complexb, value_last_complex):
    """This function is face replacing the values of a variable (var_rep_complex) in a dataframe (df) by using a condition on another variable already 
    existent in the dataframe var_rep_cond_complex. The condition is based on a list of names (list_names_complex) and the replace value is based on a
    range ( range_complex).
    Later on a new variable is generated (gen_var_complex) based on two other variables sum in the dataframe (var_cond_complex, var_rep_complex) the 
    second one being multiplied by a scalar factor (scale_complex).
    Finally a variable is generated and the values of new generated variables are replaced based on two lists (list_names_complexb, list_names_complex)
    and condition on an existing variable in the dataframe (var_rep_cond_complex) and in the end the variables receive a final value (value_last_complex)"""
    for col, i in zip(list_names_complex, range_complex):
        df.loc[df[var_rep_cond_complex]==col, var_rep_complex]=i # replace
    df[gen_var_complex] = df[var_cond_complex] + scale_complex*df[var_rep_complex] # generate
    for col1, col2 in zip(list_names_complexb, list_names_complex):
        df[col1] = 0 # generate with a zero value
        df.loc[df[var_rep_cond_complex]==col2, col1]= value_last_complex # replace
    return df

# _complex_gen_rep_mon2(df, list_names_complex, range_complex, var_rep_cond_complex, var_rep_complex, gen_var_complex, var_cond_complex, scale_complex, list_names_complexb, value_last_complex)
list_names_complex_m2 = ['Belgrano', 'Once', 'V. Crespo']
range_complex_m2 = range(1,4)
var_rep_cond_complex_m2 = 'neighborhood'
var_rep_complex_m2 = 'n_neighborhood'
gen_var_complex_m2 = 'code2'
var_cond_complex_m2 = 'month'
scale_complex_m2 = 1000
list_names_complexb_m2 = ['belgrano', 'once', 'vcrespo']
value_last_complex_m2 = 1

"""""
# gen month4=0;
MonthlyPanel2['month4'] = 0
# replace month4=1 if mes==4;
MonthlyPanel2.loc[MonthlyPanel2['month']==4, 'month4']=1
"""""

def _gen_rep_simple_mon2(df, var_gen_simple, original_val_simple, var_cond_simple, cond_simple, value_final_simple):
    """This function is first generating a new variable (var_gen_simple) in a dataframe (df) with an original value (original_val_simple)
    and after it is replacing the values of the variable based on whether another variable in the data frame (var_cond_simple) has a certain
    value (cond_simple) and then it assigns it a final value (value_final_simple)"""
    df[var_gen_simple] = original_val_simple
    df.loc[df[var_cond_simple]==cond_simple, var_gen_simple]=value_final_simple
    return df

# _gen_rep_simple_mon2(df, var_gen_simple, original_val_simple, var_cond_simple, cond_simple, value_final_simple)
var_gen_simple_m2 = 'month4'
original_val_simple_m2 = 0 
var_cond_simple_m2 = 'month'
cond_simple_m2 = 4
value_final_simple_m2 = 1
    
"""""
# gen mbelgapr=belgrano*month4; ... gen mbelgdec=belgrano*month12;
# gen monceapr=once*month4; ... gen moncedec=once*month12;
# gen mvcreapr=vcrespo*month4; ... gen mvcredec=vcrespo*month12;

list_names_place = ["mbelgapr"]
list_names_place.extend([f"mbelg{i}" for i in ["may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]], [f"monce{i}" for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]], [f"mvcre{i}" for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]])
list_names_month = ["month4"]
list_names_month.extend([f"month{i}" for i in range(5,13)])

for col1, col2 in zip(list_names_place[0:8], list_names_month[0:8]):
    MonthlyPanel2[col1] = MonthlyPanel2["belgrano"]*MonthlyPanel2[col2]
for col1, col2 in zip(list_names_place[9:17], list_names_month[0:8]):
    MonthlyPanel2[col1] = MonthlyPanel2["once"]*MonthlyPanel2[col2]
for col1, col2 in zip(list_names_place[18:26], list_names_month[0:8]):
    MonthlyPanel2[col1] = MonthlyPanel2["vcrespo"]*MonthlyPanel2[col2]
"""""

def _gen_based_variouslists_mon2(df, list_names_place, list_names_month, list_names_variouslists):
    """This function is firstly extending two lists which in turn will be used to create a set of variables (list_names_place) based on the values of two different 
    other lists (list_names_month,  list_names_variouslists_m2) multiplied by each other"""
    list_names_place.extend([f"mbelg{i}" for i in ["may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]], [f"monce{i}" for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]], [f"mvcre{i}" for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]])
    list_names_month.extend([f"month{i}" for i in range(5,13)])
    for i in range(0,3):
        for col1, col2 in zip(list_names_place[0:8], list_names_month[0:8]):
            df[col1] = df[list_names_variouslists[i]]*df[col2]
    return df
            
            
# _gen_based_variouslists_mon2(df, list_names_place, list_names_month, list_names_variouslists_m2)
list_names_place_m2 = ["mbelgapr"]
list_names_month_m2 = ["month4"]      
list_names_variouslists_m2 = ['belgrano', 'once', 'vcrespo']
  
"""""
# Saving the data frame
MonthlyPanel2.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel2.csv')
"""""

def _df_to_csv_mon2(df, location):
    "This functions saves a dataframe (df) as a csv file ina specific location (location)"
    df.to_csv(location)
    
# _df_to_csv_mon2(df, location)
location_m2 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel2.csv'

#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_2(location_origin,
                   var_drop, drop1, drop2,
                   new_var_v_cond, ori_var_v_cond, var_con_v_cond, con_v_cond1, con_v_cond2, con_v_cond3, con_v_cond4, con_v_cond5, multiple1_v_cond, multiple2_v_cond,
                   list_for_NA, var_con_NA, fixed_var_NA, NA_value,
                   list_sort,
                   new_egen_var, by_var, var_egen_sup, cond_ege_var, cond_ege_val, ege_var_change, ege_scale_factor,
                   new_egen_var2, by_var2, var_egen_sup2, cond_ege_var2, cond_ege_val2, ege_var_change2, ege_scale_factor2,
                   new_generated_var, existing_var, scalar_gen,
                   new_gen_var_sim, value_sim,
                   new_gen_var_sim2, value_sim2,
                   list_names_complex, range_complex, var_rep_cond_complex, var_rep_complex, gen_var_complex, var_cond_complex, scale_complex, list_names_complexb, value_last_complex,
                   var_gen_simple, original_val_simple, var_cond_simple, cond_simple, value_final_simple,
                   list_names_place, list_names_month, list_names_variouslists,
                   location):
    df = _reading_data_csv(location_origin)
    df = _drop_if_simple_mon2(df, var_drop, drop1, drop2)
    df = _gen_rep_various_cond_mon2(df, new_var_v_cond, ori_var_v_cond, var_con_v_cond, con_v_cond1, con_v_cond2, con_v_cond3, con_v_cond4, con_v_cond5, multiple1_v_cond, multiple2_v_cond)
    df = _genNA_rep_two_cond_mon2(df, list_for_NA, var_con_NA, fixed_var_NA, NA_value)
    df = _sort_mon2(df, list_sort)
    df = _egen_rep_mon2(df, new_egen_var, by_var, var_egen_sup, cond_ege_var, cond_ege_val, ege_var_change, ege_scale_factor)
    df = _egen_rep2_mon2(df, new_egen_var2, by_var2, var_egen_sup2, cond_ege_var2, cond_ege_val2, ege_var_change2, ege_scale_factor2)
    df = _gen_simplebased_mon2(df, new_generated_var, existing_var, scalar_gen)
    df = _gen_simple_mon2(df, new_gen_var_sim, value_sim)
    df = _gen_simple2_mon2(df, new_gen_var_sim2, value_sim2)
    df = _complex_gen_rep_mon2(df, list_names_complex, range_complex, var_rep_cond_complex, var_rep_complex, gen_var_complex, var_cond_complex, scale_complex, list_names_complexb, value_last_complex)
    df = _gen_rep_simple_mon2(df, var_gen_simple, original_val_simple, var_cond_simple, cond_simple, value_final_simple)
    df = _gen_based_variouslists_mon2(df, list_names_place, list_names_month, list_names_variouslists) # list_names_variouslists_m2
    _df_to_csv_mon2(df, location) 
    
### FUNCTION ###

MonthlyPanel2 = monthlypanel_2(location_origin=location_origin_m2, 
                   var_drop=var_drop_m2, drop1=drop1_m2, drop2=drop2_m2,
                   new_var_v_cond=new_var_v_cond_m2, ori_var_v_cond=ori_var_v_cond_m2, var_con_v_cond=var_con_v_cond_m2, con_v_cond1=con_v_cond1_m2, con_v_cond2=con_v_cond2_m2, con_v_cond3=con_v_cond3_m2, con_v_cond4=con_v_cond4_m2, con_v_cond5= con_v_cond5_m2, multiple1_v_cond=multiple1_v_cond_m2, multiple2_v_cond=multiple2_v_cond_m2,
                   list_for_NA=list_for_NA_m2, var_con_NA=var_con_NA_m2, fixed_var_NA=fixed_var_NA_m2, NA_value=NA_value_m2,
                   list_sort=list_sort_m2,
                   new_egen_var=new_egen_var_m2, by_var=by_var_m2, var_egen_sup=var_egen_sup_m2, cond_ege_var=cond_ege_var_m2, cond_ege_val=cond_ege_val_m2, ege_var_change=ege_var_change_m2, ege_scale_factor=ege_scale_factor_m2,
                   new_egen_var2=new_egen_var2_m2, by_var2=by_var2_m2, var_egen_sup2=var_egen_sup2_m2, cond_ege_var2=cond_ege_var2_m2, cond_ege_val2=cond_ege_val2_m2, ege_var_change2=ege_var_change2_m2, ege_scale_factor2=ege_scale_factor2_m2,
                   new_generated_var=new_generated_var_m2, existing_var=existing_var_m2, scalar_gen=scalar_gen_m2,
                   new_gen_var_sim=new_gen_var_sim_m2, value_sim=value_sim_m2,
                   new_gen_var_sim2= new_gen_var_sim2_m2, value_sim2=value_sim2_m2,
                   list_names_complex=list_names_complex_m2, range_complex=range_complex_m2, var_rep_cond_complex=var_rep_cond_complex_m2, var_rep_complex=var_rep_complex_m2, gen_var_complex=gen_var_complex_m2, var_cond_complex=var_cond_complex_m2, scale_complex=scale_complex_m2, list_names_complexb=list_names_complexb_m2, value_last_complex=value_last_complex_m2,
                   var_gen_simple=var_gen_simple_m2, original_val_simple=original_val_simple_m2, var_cond_simple=var_cond_simple_m2, cond_simple=cond_simple_m2, value_final_simple=value_final_simple_m2,
                   list_names_place=list_names_place_m2, list_names_month=list_names_month_m2, list_names_variouslists=list_names_variouslists_m2,
                   location=location_m2)


############################################## PART 3 ########################################################################################################################

"""""
### Reading the data ###
MonthlyPanel2 = pd.read_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel2.csv')
"""""

def _reading_data_csv(location_origin):
    df = pd.read_csv(location_origin)
    return df

# _reading_data_csv(location_origin)
location_origin_m3 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel2.csv'
    
"""""
# drop month4;
MonthlyPanel3 = MonthlyPanel2.drop(columns='month4')
"""""

def _single_drop(df, column_to_drop):
    df = df.drop(columns=column_to_drop)
    return df

# def _single_drop(df, column_to_drop):
column_to_drop_m3 = 'month4'

"""""
# gen todos=0;
MonthlyPanel3['all_locations'] = 0
# replace todos=1 if (edpub==1 | estserv==1 | banco==1);
MonthlyPanel3.loc[(MonthlyPanel3['public_building_or_embassy']==1) | (MonthlyPanel3['gas_station']==1) | (MonthlyPanel3['bank']==1), 'all_locations']=1
"""""

def _gen_rep_3cond_mon3(df, gen_var_3cond, initial_val_3cond, col1_3cond, col2_3cond, col3_3cond, global_replace_val_3cond):
    """This function is generating a variable (gen_var_3cond) in a dataframe (df) with an initial value initial_val_3cond.
    Then it is replacing the value of this variable if any of three condition on three variables are met (col1_3cond, col2_3cond, col3_3cond)
    The values they need to meet and the value of the new variable in the end is the same (global_replace_val_3cond)"""
    df[gen_var_3cond] = initial_val_3cond # generate
    df.loc[(df[col1_3cond]==global_replace_val_3cond) | (df[col2_3cond]==global_replace_val_3cond) | (df[col3_3cond]==global_replace_val_3cond), gen_var_3cond]=global_replace_val_3cond #replace
    return df

# _gen_rep_3cond_mon3(df, gen_var_3cond, initial_val_3cond, col1_3cond, col2_3cond, col3_3cond, global_replace_val_3cond)
gen_var_3cond_m3 = 'all_locations'
initial_val_3cond_m3 = 0
col1_3cond_m3 = 'public_building_or_embassy'
col2_3cond_m3 = 'gas_station'
col3_3cond_m3 = 'bank'
global_replace_val_3cond_m3 = 1 

"""""
# gen epin1p=edpub*inst1p; gen epin3_1p=edpub*inst3_1p; gen epcuad2p=edpub*cuad2p; gen nepin1p=(1-edpub)*inst1p; gen nepi3_1p=(1-edpub)*inst3_1p; gen nepcua2p=(1-edpub)*cuad2p;
# gen esin1p=estserv*inst1p; gen esin3_1p=estserv*inst3_1p; gen escuad2p=estserv*cuad2p; gen nesin1p=(1-estserv)*inst1p; gen nesi3_1p=(1-estserv)*inst3_1p; gen nescua2p=(1-estserv)*cuad2p;
# gen bain1p=banco*inst1p; gen bain3_1p=banco*inst3_1p; gen bacuad2p=banco*cuad2p; gen nbain1p=(1-banco)*inst1p; gen nbai3_1p=(1-banco)*inst3_1p; gen nbacua2p=(1-banco)*cuad2p;
# gen toin1p=todos*inst1p; gen toin3_1p=todos*inst3_1p; gen tocuad2p=todos*cuad2p; gen ntoin1p=(1-todos)*inst1p; gen ntoi3_1p=(1-todos)*inst3_1p; gen ntocua2p=(1-todos)*cuad2p;
list_names_data3_general = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p'] 
list_names_data3_1 = ['public_building_or_embassy_p', 'public_building_or_embassy_1_p', 'public_building_or_embassy_cuad2p', 'n_public_building_or_embassy_p', 'n_public_building_or_embassy_1_p', 'n_public_building_or_embassy_cuad2p']
list_names_data3_2 = ['gas_station_p', 'gas_station_1_p', 'gas_station_cuad2p', 'n_gas_station_p', 'n_gas_station_1_p', 'n_gas_station_cuad2p']
list_names_data3_3 = ['bank_p', 'bank_1_p', 'bank_cuad2p', 'n_bank_p', 'n_bank_1_p', 'n_bank_cuad2p']
list_names_data3_4 = ['all_locations_p', 'all_locations_1_p', 'all_locations_cuad2p', 'n_all_locations_p', 'n_all_locations_1_p', 'n_all_locations_cuad2p']

list_names_3_variables = ['public_building_or_embassy', 'gas_station', 'bank', 'all_locations']

for i, j in zip(range(1,5), list_names_3_variables):
    for col1, col2 in zip(f"list_names_data3_{i}", list_names_data3_general):
        MonthlyPanel3[col1] = MonthlyPanel3[j] * MonthlyPanel3[col2] 
"""""

def _gen_multiplevariables_listbased(df, list_value1, list_value2, list_value3, list_value4, list_names_3_variables, list_names_data3_general):
    """This function is list based. Given some list entries (list_value1, list_value2, list_value3, list_value4, list_names_3_variables, list_names_data3_general), 
    various variables are created in a data frame (df)."""
    list_names_data3_1 = list_value1
    list_names_data3_2 = list_value2
    list_names_data3_3 = list_value3
    list_names_data3_4 = list_value4
    for i, j in zip(range(1,5), list_names_3_variables):
        for col1, col2 in zip(f"list_names_data3_{i}", list_names_data3_general):
            df[col1] = df[j] * df[col2] 
    return df

# _gen_multiplevariables_listbased(df, list_value1, list_value2, list_value3, list_value4, list_names_3_variables, list_names_data3_general)
list_value1_m3 = ['public_building_or_embassy_p', 'public_building_or_embassy_1_p', 'public_building_or_embassy_cuad2p', 'n_public_building_or_embassy_p', 'n_public_building_or_embassy_1_p', 'n_public_building_or_embassy_cuad2p']
list_value2_m3 = ['gas_station_p', 'gas_station_1_p', 'gas_station_cuad2p', 'n_gas_station_p', 'n_gas_station_1_p', 'n_gas_station_cuad2p']
list_value3_m3 = ['bank_p', 'bank_1_p', 'bank_cuad2p', 'n_bank_p', 'n_bank_1_p', 'n_bank_cuad2p']
list_value4_m3 = ['all_locations_p', 'all_locations_1_p', 'all_locations_cuad2p', 'n_all_locations_p', 'n_all_locations_1_p', 'n_all_locations_cuad2p']
list_names_3_variables_m3 = ['public_building_or_embassy', 'gas_station', 'bank', 'all_locations']
list_names_data3_general_m3 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p'] 


"""""
# Saving the data frame
MonthlyPanel3.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel3.csv')
"""""

def _df_to_csv_mon3(df, location):
    "This functions saves a dataframe (df) as a csv file ina specific location (location)"
    df.to_csv(location)
    
# _df_to_csv_mon3(df, location)
location_m3 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel3.csv'

#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_3(location_origin, 
                   column_to_drop,
                   gen_var_3cond, initial_val_3cond, col1_3cond, col2_3cond, col3_3cond, global_replace_val_3cond,
                   list_value1, list_value2, list_value3, list_value4, list_names_3_variables, list_names_data3_general,
                   location):
    df = _reading_data_csv(location_origin)
    df = _single_drop(df, column_to_drop)
    df = _gen_rep_3cond_mon3(df, gen_var_3cond, initial_val_3cond, col1_3cond, col2_3cond, col3_3cond, global_replace_val_3cond)
    df = _gen_multiplevariables_listbased(df, list_value1, list_value2, list_value3, list_value4, list_names_3_variables, list_names_data3_general)
    _df_to_csv_mon3(df, location)

### FUNCTION ###

MonthlyPanel3 = monthlypanel_3(location_origin=location_origin_m3, 
                               column_to_drop=column_to_drop_m3,
                               gen_var_3cond=gen_var_3cond_m3, initial_val_3cond=initial_val_3cond_m3, col1_3cond=col1_3cond_m3, col2_3cond=col2_3cond_m3, col3_3cond=col3_3cond_m3, global_replace_val_3cond=global_replace_val_3cond_m3,
                               list_value1=list_value1_m3, list_value2=list_value2_m3, list_value3=list_value3_m3, list_value4=list_value4_m3, list_names_3_variables=list_names_3_variables_m3, list_names_data3_general=list_names_data3_general_m3,
                               location=location_m3) 

############################################## PART 4 ########################################################################################################################
    
# drop all variables created before
MonthlyPanel4 = MonthlyPanel3.drop(columns=[list_names_data3_1, list_names_data3_2, list_names_data3_3, list_names_data3_4], inplace=True)

######################################################################################################################################################################

# use "C:\_ernesto\Crime\AER_DataFiles&Programs\MonthlyPanel.dta", clear;
MonthlyPanel_new, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/march2004_ditella_data/MonthlyPanel.dta')

### Renaming columns ###

def _clean_column_names_mon4(df):
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
        .str.replace("mes", "month")
    )
    return df

# _clean_column_names_mon3(df)
df_m4 = MonthlyPanel_new

"""""
# gen inst3_1=institu3-institu1;
MonthlyPanel_new['jewish_inst_one_block_away_1'] = MonthlyPanel_new['jewish_inst_one_block_away'] - MonthlyPanel_new['jewish_inst']
"""""

def _gen_var_difference_mon4(df, new_var_new, var1_new, var_sub_new):
    
    """This function generates a new variable (new_var) in a dataframe (df) using existing columns of the dataframe (var1, var_sub) and substracting them.
    var_sub is the column being substracted"""
    df[new_var_new] = df[var1_new] - df[var_sub_new]
    return df

# _gen_var_difference_mon(df, new_var, var1, var_sub)
new_var_new_m4 = 'jewish_inst_one_block_away_1'
var1_new_m4 = 'jewish_inst_one_block_away'
var_sub_new_m4 = 'jewish_inst'

"""""
# gen cuad2=0; gen month5=0; gen month6=0; gen month7=0;
list_new = ['cuad2', 'month5', 'month6', 'month7']
MonthlyPanel_new[[col for col in list_new]] = 0
"""""

def _gen_variouslisted_var_mon4(df, list_various_gen, original_val_various_list):
    """This function is generating a new set of variables from a list (list_various_gen) into a dataframe (df) giving all of them an original value (original_val_various_list)"""
    df[[col for col in list_various_gen]] = original_val_various_list
    return df
    
# _gen_variouslisted_var_mon4(df, list_various_gen, original_val_various_list)
list_various_gen_m4 = ['cuad2', 'month5', 'month6', 'month7']
original_val_various_list_m4 = 0
    
"""""
# replace cuad2=1 if distanci==2;
MonthlyPanel_new.loc[MonthlyPanel_new['distance_to_jewish_inst']==2, 'cuad2'] = 1
"""""

def _rep_simple_mon4(df, cond_var_simple, cond_val_simple, var_simple_rep, val_assigned_simple):
    """This function is replacing a variable (var_simple_rep) in a dataframe (df) given that an existing variable in the dataframe (cond_var_simple) should meet a conditional
    value (cond_val_simple). If the condition is met, the variable gets assigned a value (val_assigned_simple)"""
    df.loc[df[cond_var_simple]==cond_val_simple, var_simple_rep] = val_assigned_simple
    return df

# _rep_simple_mon4(df, cond_var_simple, cond_val_simple, var_simple_rep, val_assigned_simple)
cond_var_simple_m4 = 'distance_to_jewish_inst'
cond_val_simple_m4 = 2
var_simple_rep_m4 = 'cuad2'
val_assigned_simple_m4 = 1
    
"""""
# replace month5=1 if mes==5; replace month6=1 if mes==6; replace month7=1 if mes==7;
for i in range (5,8):
    MonthlyPanel_new.loc[MonthlyPanel_new['month']==i, f'month{i}'] = 1
"""""

def _rep_various_mon4(df, range_replace, cond_var_simple_v, variables_replace, val_assigned_various):
    """This function is replacing various variables (variables_replace) in a dataframe (df) by means of using a loop on a fixed range (range_replace), based on a condition
    that should be met by another variable in the dataframe (cond_var_simple_v). If the condition is met, the value assigned to this variables is also fixed (val_assigned_various)"""
    for i in range_replace:
        df.loc[df[cond_var_simple_v]==i, variables_replace] = val_assigned_various
    return df

# _rep_various_mon4(df, range_replace, cond_var_simple_v, variables_replace, val_assigned_various)
range_replace_m4 = range (5,8)
cond_var_simple_v_m4 = 'month'
variables_replace_m4 = f'month{i}'
val_assigned_various_m4 = 1
    
"""""    
# drop if mes==72; mes==73; mes==8; mes==9; mes==10; mes==11; mes==12;
MonthlyPanel_new.drop(MonthlyPanel_new[(MonthlyPanel_new['month']==72) | (MonthlyPanel_new['month']==73) | (MonthlyPanel_new['month']==8) |(MonthlyPanel_new['month']==9) | 
                                       (MonthlyPanel_new['month']==10) | (MonthlyPanel_new['month']==11) | (MonthlyPanel_new['month']==12)].index, inplace = True)
"""""

def _drop_conditional(df, var_drop_cond, drop_condition_list):
    """This function is simply droping values of a dataframe (df) for which a variable (var_drop_cond) meets a ceratin condition given by a list (drop_condition_list)"""
    df.drop(df[df[var_drop_cond].isin(drop_condition_list)].index, inplace=True)
    return df

# _drop_conditional(df, var_drop_cond, drop_condition_list)
var_drop_cond_m4 = 'month'
drop_condition_list_m4 = [72, 73, 8, 9, 10, 11, 12]

"""""
# gen post1=0; gen post2=0; gen post3=0;
list_post = ['post1', 'post2', 'post3']
# replace post1=1 if mes>4; replace post2=1 if mes>5; replace post3=1 if mes>6;
for col, i in zip(list_post, range(4,7)):
    MonthlyPanel_new.loc[MonthlyPanel_new['month']>i, col] = 1
"""""
    
def _gen_rep_var_various_cond_biggerthan_mon4(df, list_gen_var_cond, original_value_var_cond, range_var_cond, cond_var_cond, final_value_var_cond):
    
    """This function tries to generate a set of variables (list_gen_var_cond) in a data frame (df) with an original (original_value_var_cond) value to later on replace it 
    given a condition of another column (cond_var_cond) given it a specific value (final_value_var_cond) and all of this is dependent on a loop range (range_var_cond)"""
    for col in list_gen_var_cond:
        df[col] = original_value_var_cond
    for col, i in zip(list_gen_var_cond, range_var_cond):
        df.loc[df[cond_var_cond]>i, col] = final_value_var_cond
    return df

# _gen_rep_var_various_cond_biggerthan_mon4(df, list_gen_var_cond, original_value_var_cond, range_var_cond, cond_var_cond, final_value_var_cond)
list_gen_var_cond_m4 = ['post1', 'post2', 'post3']
original_value_var_cond_m4 = 0
range_var_cond_m4 = range(4,7)
cond_var_cond_m4 = 'month'
final_value_var_cond_m4 = 1

"""""
# gen inst1p=institu1*post1; gen inst3_1p=inst3_1*post1; gen cuad2p=cuad2*post1;
# gen inst1p=institu1*post2; gen inst3_1p=inst3_1*post2; gen cuad2p=cuad2*post2;
# gen inst1p=institu1*post3; gen inst3_1p=inst3_1*post3; gen cuad2p=cuad2*post3;

list_var_gen = ['jewish_inst', 'jewish_inst_one_block_away_1', 'cuad2']
list_new_var = ['1jewish_inst_1_p', '1jewish_inst_one_block_away_1_p', '1cuad2p', '2jewish_inst_1_p', '2jewish_inst_one_block_away_1_p', '2cuad2p', '3jewish_inst_1_p', '3jewish_inst_one_block_away_1_p', '3cuad2p']

for i in [0,3,6]:
    for col1, col2, col3 in zip(list_var_gen, list_post, list_new_var[i:i+2]):
        MonthlyPanel_new[col3] = MonthlyPanel_new[col1] * MonthlyPanel_new[col2]
"""""

def _gen_specificrule_list(df, range_specific_loop, list_var_gen_spec, list_var_ext_spec, list_new_var_spec):
    """This function is generating specific new variables (ist_var_gen_spec) in a dataframe (df) based on already existing set of variables in the data frame
    (list_var_ext_spec, list_new_var_spec) and this is done over a loop range(range_specific_loop)"""
    for i in range_specific_loop:
        for col1, col2, col3 in zip(list_var_gen_spec, list_var_ext_spec, list_new_var_spec[i:i+2]):
            df[col3] = df[col1] * df[col2]
    return df
            
# _gen_specificrule_list(df, range_specific_loop, list_var_gen_spec, list_var_ext_spec, list_new_var_spec)
range_specific_loop_m4 = [0,3,6]
list_var_gen_spec_m4 = ['jewish_inst', 'jewish_inst_one_block_away_1', 'cuad2'] 
list_var_ext_spec_m4 = ['post1', 'post2', 'post3']
list_new_var_spec_m4 = ['one_jewish_inst_1_p', 'one_jewish_inst_one_block_away_1_p', 'one_cuad2p', 'two_jewish_inst_1_p', 'two_jewish_inst_one_block_away_1_p', 'two_cuad2p', 'three_jewish_inst_1_p', 'three_jewish_inst_one_block_away_1_p', 'three_cuad2p']
    
"""""
MonthlyPanel_new = MonthlyPanel_new.dropna()
"""""

def _dropna_mon4(df):
    "This function is just dropping all NA values in our dataframe"
    df = df.dropna()
    return df

"""""
# Saving the data frame
MonthlyPanel_new.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel_new.csv')
"""""

def _df_to_csv_mon4(df, location):
    "This functions saves a dataframe (df) as a csv file ina specific location (location)"
    df.to_csv(location)
    
# _df_to_csv_mon3(df, location)
location_m4 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/MonthlyPanel_new.csv'

#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_4(df,
                   new_var_new, var1_new, var_sub_new,
                   list_various_gen, original_val_various_list,
                   cond_var_simple, cond_val_simple, var_simple_rep, val_assigned_simple,
                   range_replace, cond_var_simple_v, variables_replace, val_assigned_various,
                   var_drop_cond, drop_condition_list,
                   list_gen_var_cond, original_value_var_cond, range_var_cond, cond_var_cond, final_value_var_cond,
                   range_specific_loop, list_var_gen_spec, list_var_ext_spec, list_new_var_spec,
                   location):
    df = _clean_column_names_mon4(df)
    df = _gen_var_difference_mon4(df, new_var_new, var1_new, var_sub_new)
    df = _gen_variouslisted_var_mon4(df, list_various_gen, original_val_various_list)
    df = _rep_simple_mon4(df, cond_var_simple, cond_val_simple, var_simple_rep, val_assigned_simple)
    df = _rep_various_mon4(df, range_replace, cond_var_simple_v, variables_replace, val_assigned_various)
    df = _drop_conditional(df, var_drop_cond, drop_condition_list)
    df = _gen_rep_var_various_cond_biggerthan_mon4(df, list_gen_var_cond, original_value_var_cond, range_var_cond, cond_var_cond, final_value_var_cond)
    df = _gen_specificrule_list(df, range_specific_loop, list_var_gen_spec, list_var_ext_spec, list_new_var_spec)
    df = _dropna_mon4(df)
    _df_to_csv_mon4(df, location)
    
### FUNCTION ###

MonthlyPanel_new = monthlypanel_4(df = MonthlyPanel_new,
                   new_var_new = new_var_new_m4, var1_new=var1_new_m4, var_sub_new=var_sub_new_m4,
                   list_various_gen=list_various_gen_m4, original_val_various_list=original_val_various_list_m4,
                   cond_var_simple=cond_var_simple_m4, cond_val_simple=cond_val_simple_m4, var_simple_rep=var_simple_rep_m4, val_assigned_simple=val_assigned_simple_m4,
                   range_replace=range_replace_m4, cond_var_simple_v=cond_var_simple_v_m4, variables_replace=variables_replace_m4, val_assigned_various=val_assigned_various_m4,
                   var_drop_cond=var_drop_cond_m4, drop_condition_list=drop_condition_list_m4,
                   list_gen_var_cond=list_gen_var_cond_m4, original_value_var_cond=original_value_var_cond_m4, range_var_cond=range_var_cond_m4, cond_var_cond=cond_var_cond_m4, final_value_var_cond=final_value_var_cond_m4,
                   range_specific_loop=range_specific_loop_m4, list_var_gen_spec=list_var_gen_spec_m4, list_var_ext_spec=list_var_ext_spec_m4, list_new_var_spec=list_new_var_spec_m4,
                   location=location_m4)







""" Weekly Panel """

### Reading the data ###

WeeklyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/WeeklyPanel.dta')

### Renaming columns ###

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

def _drop_variables(df, list_drop):
    df.drop(columns=list_drop, inplace=True)

# def _drop_variables(df, list_drop)
list_drop_we = ['street', 'street_nr', 'public_building_or_embassy', 'gas_station', 'bank']

"""""
# Generate variables 

"fixed extension"
# gen week1=0; ... gen week39=0;
list_names = ["week1"]
list_names.extend([f"week{i}" for i in range(2,40)])
WeeklyPanel[[col for col in list_names]] = 0
# replace semana1=1 if week==1; ... replace semana39=1 if week==39;
for i in range(1,40):
    WeeklyPanel.loc[WeeklyPanel['week']==i, f"week{i}"]=1
    
"fixed list simple"
# gen cuad2=0; # gen post=0; # gen n_neighborhood=0;
list1 = ["cuad2", "post", "n_neighborhood"]
WeeklyPanel[[col for col in list1]] = 0
# replace cuad2=1 if distance_to_jewish_inst==2;
WeeklyPanel.loc[WeeklyPanel['distance_to_jewish_inst']==2, 'cuad2']=1

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
    
def _gen_rep_variables_fixedextension_we(df,list_names_ext, ext_cond, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext):    
    """"This functions has certain inputs to generate a variable and replace its values (list_names_ext, ext_cond, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext)""" 
    for i in range_ext:
        list_names_ext.extend([ext_cond])  
    df[[col for col in list_names_ext]] = original_value_var # generate
    for i in range_loop:
        df.loc[df[var_cond_ext]==i, ext_cond]= final_value_var # replace
    return df

# _gen_rep_variables_we(df, type_of_list, list_names_ext, ext_cond, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext) # FIXED EXTENSION
list_names_ext_we = ["week1"]
ext_cond_we = f"week{i}"
range_ext_we = range(2,40)
original_value_var_we = 0
final_value_var_we = 1
range_loop_we = range(1,40)
var_cond_ext_we = 'week'


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
# replace post=1 if week>=18;
WeeklyPanel.loc[WeeklyPanel['week']>18, 'post']=1
"""""
        
def _rep_variables(df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace):
    
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
    
def _gen_diff_diff_variables(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_d, var1_d, var2_d, factor1_d, factor2_d)"""
    df[new_var_d] = (factor1_d*df[var1_d]) - (factor2_d*df[var2_d])
    return df

# _gen_diff_diff_variables(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d)
new_var_d_we = 'jewish_int_one_block_away_1'
var1_d_we = 'jewish_inst_one_block_away'
var2_d_we = 'jewish_inst'
factor1_d_we = 1
factor2_d_we = 1

def _gen_diff_sum_variables(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_s, var1_s, var2_s, factor1_s, factor2_s)"""
    df[new_var_s] = (factor1_s*df[var1_s]) + (factor2_s*df[var2_s])
    return df

# _gen_diff_sum_variables(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s)
new_var_s_we = 'code2'
var1_s_we = 'week'
var2_s_we = 'n_neighborhood'
factor1_s_we = 1
factor2_s_we = 1000

def _gen_diff_multiplication_variables(df, new_var_m, var1_m, var2_m, factor1_m, factor2_m):
    """This function is creating a variable based on a difference of already existing variables in a dataframe. It has the following inputs (new_var_m, var1_m, var2_m, factor1_m, factor2_m)"""
    df[new_var_m] = (factor1_m*df[var1_m]) * (factor2_m*df[var2_m])
    return df

# _gen_diff_multiplication_variables(df, new_var_m, var1_m, var2_m, factor1_m, factor2_m)
new_var_m_we = 'n_total_thefts'
var1_m_we = 'total_thefts'
var2_m_we = (365/12)/7 
factor1_m_we = 1 
factor2_m_we = 1
  
"""""
# Saving the data frame
WeeklyPanel.to_csv('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/WeeklyPanel.csv')
"""""

def _df_to_csv(df, location):
    "This functions saves a dataframe (df) as a csv file ina specific location (location)"
    df.to_csv(location)
    
# _df_to_csv(df, location)
location_we = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/clean data/WeeklyPanel.csv' 

#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def weeklypanel(df, 
                list_drop,
                list_names_ext, ext_cond, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext,
                list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix,
                list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change,
                type_of_condition, var_cond_rep, condition_num, replace_var, value_replace,
                new_var_d, var1_d, var2_d, factor1_d, factor2_d,
                new_var_s, var1_s, var2_s, factor1_s, factor2_s,
                new_var_m, var1_m, var2_m, factor1_m, factor2_m,
                location):
    df = _clean_column_names(df)
    df = _drop_variables(df, list_drop)
    df = _gen_rep_variables_fixedextension_we(df,list_names_ext, ext_cond, range_ext, original_value_var, final_value_var, range_loop, var_cond_ext)
    df = _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, cond_fix, var_fix, value_var_fix)
    df = _gen_rep_variables_fixedlistcomplex_we(df, list1_fix_com, list2_fix_com, var_fix_comp_mul, range_rep_fix_com, list_rep_fix_com, var_fix_com_to_use, var_fix_com_to_change)
    df = _rep_variables(df, type_of_condition, var_cond_rep, condition_num, replace_var, value_replace)
    df = _gen_diff_diff_variables(df, new_var_d, var1_d, var2_d, factor1_d, factor2_d)
    df = _gen_diff_sum_variables(df, new_var_s, var1_s, var2_s, factor1_s, factor2_s)
    df = _gen_diff_multiplication_variables(df, new_var_m, var1_m, var2_m, factor1_m, factor2_m)
    _df_to_csv(df, location)
    
WeeklyPanel = weeklypanel(df=WeeklyPanel, 
                list_drop=list_drop_we,
                list_names_ext=list_names_ext_we, ext_cond=ext_cond_we, range_ext=range_ext_we, original_value_var=original_value_var_we, final_value_var=final_value_var_we, range_loop=range_loop_we, var_cond_ext=var_cond_ext_we,
                list_fixed=list_fixed_we, var_cond_fix=var_cond_fix_we, cond_fix=cond_fix_we, var_fix=var_fix_we, value_var_fix=value_var_fix_we,
                list1_fix_com=list1_fix_com_we, list2_fix_com=list2_fix_com_we, var_fix_comp_mul=var_fix_comp_mul_we, range_rep_fix_com=range_rep_fix_com_we, list_rep_fix_com=list_rep_fix_com_we, var_fix_com_to_use=var_fix_com_to_use_we, var_fix_com_to_change=var_fix_com_to_change_we,
                type_of_condition=type_of_condition_we, var_cond_rep=var_cond_rep_we, condition_num=condition_num_we, replace_var=replace_var_we, value_replace=value_replace_we,
                new_var_d=new_var_d_we, var1_d=var1_d_we, var2_d=var2_d_we, factor1_d=factor1_d_we, factor2_d=factor2_d_we,
                new_var_s=new_var_s_we, var1_s=var1_s_we, var2_s=var2_s_we, factor1_s=factor1_s_we, factor2_s=factor2_s_we,
                new_var_m=new_var_m_we, var1_m=var1_m_we, var2_m=var2_m_we, factor1_m=factor1_m_we, factor2_m=factor2_m_we,
                location=location_we)





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
