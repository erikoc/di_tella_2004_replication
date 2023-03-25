"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd
import pyreadstat  as pyread



""" Monthly Panel """ 

MonthlyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/MonthlyPanel.dta')


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


df_m1 = MonthlyPanel



############################################## PART 1 ########################################################################################################################

def _gen_rep_var_fixed_extension_mon(df, var_cond_ext, range_loop=range(5,13), original_value_var=0, final_value_var=1):
    
    """
    Generate new variables based on a condition and replace values of the variable based on another condition.
    
    Args:
    - df: pandas dataframe
    - var_cond_ext: name of the column in the dataframe to use as condition for replacing the variable
    - range_loop: range of values to use for generating and replacing the variable (default: range(5, 13))
    - original_value_var: value to be assigned to the generated variable (default: 0)
    - final_value_var: value to replace the variable with (default: 1)
    
    Returns:
    - df: pandas dataframe with new variables generated and specified variable replaced based on conditions
    """
    
    for i in range_loop:
        list_names_ext = []
        list_names_ext.extend([f"month{i}"])  
        df[[col for col in list_names_ext]] = original_value_var
    for i in range_loop:
        df.loc[df[var_cond_ext]==i, f"month{i}"]= final_value_var
    return df

var_cond_ext_m1 = 'month'






def _gen_var_difference_mon(df, new_var, var1, var_sub):
    
    """
    Generate a new variable in a dataframe by subtracting one column from another.

    Args:
    - df: pandas dataframe
    - new_var: name of the new variable to be generated
    - var1: name of the column to subtract from
    - var_sub: name of the column to subtract

    Returns:
    - df: pandas dataframe with the new variable generated
    """
    
    df[new_var] = df[var1] - df[var_sub]
    return df

new_var_m1 = 'jewish_inst_one_block_away_1'
var1_m1 = 'jewish_inst_one_block_away'
var_sub_m1 = 'jewish_inst'
    




def _gen_rep_var_single_cond_biggerthan_mon(df, var_gen, cond_var, original_value=0, final_value=1, cond=7):
    
    """Generate a new variable `var_gen` in a pandas dataframe `df` with an original value `original_value`. 
    If the condition variable `cond_var` is greater than the condition `cond`, the variable will be replaced with `final_value`.

    Args:
    - df: pandas dataframe
    - var_gen: name of the new variable to generate
    - cond_var: name of the column in the dataframe to use as condition for replacement
    - original_value: original value to be assigned to the new variable (default: 0)
    - final_value: value to replace the new variable if the condition is met (default: 1)
    - cond: value of the condition variable `cond_var` to check for (default: 7)

    Returns:
    - df: pandas dataframe with new variable generated and specified variable replaced based on conditions
    """
    
    df[var_gen] = original_value
    df.loc[df[cond_var]>cond, var_gen]= final_value
    return df

var_gen_m1 = 'post'
cond_var_m1 = 'month'


    


def _gen_rep_var_fixed_extension_mon2(df, var_cond_ext2, original_value_var2=0, range_loop2=range(0,8), final_value_var2=1):
    
    """
    Generate new variables based on a condition and replace values of the variable based on another condition.
    
    Args:
    - df: pandas dataframe
    - var_cond_ext2: name of the column in the dataframe to use as condition for replacing the variable
    - range_loop2: range of values to use for generating and replacing the variable (default: range(0, 8))
    - original_value_var2: value to be assigned to the generated variable (default: 0)
    - final_value_var2: value to replace the variable with (default: 1)
    
    Returns:
    - df: pandas dataframe with new variables generated and specified variable replaced based on conditions
    """
    
    for i in range_loop2:
        list_names_ext2=[]
        list_names_ext2.extend([f"cuad{i}"])  
        df[[col for col in list_names_ext2]] = original_value_var2 
    for i in range_loop2:
        df.loc[df[var_cond_ext2]==i, f"cuad{i}"]= final_value_var2 
    return df


var_cond_ext2_m1 = 'distance_to_jewish_inst'





def _gen_var_cond_list_similar_mon(df, ori_variables, fixed_variable, name_change=f'p'):
    
    """
    Generates new variables in a dataframe based on existing variables with a similar name by multiplying them with a fixed variable.
    
    Args:
    - df: pandas dataframe
    - ori_variables: list of column names in the dataframe to generate new variables from
    - fixed_variable: variable to be used as a multiplier in the multiplication
    - name_change: string to be added to the end of each new variable name (default: 'p')
    
    Returns:
    - df: pandas dataframe with new variables generated
    """
    
    for col in ori_variables:
        df[col+name_change] = df[col]*df[fixed_variable]
    return df


ori_variables_m1 = ["cuad0", "cuad1", "cuad2", "cuad3", "cuad4", "cuad5", "cuad6", "cuad7"]
fixed_variable_m1 = "post"
 
 
 




def _gen_rep_var_various_cond_equality_mon(df, new_gen_variable, list_ext_variables, new_original_value=4, range_new_gen=range(1,4), value_originallist=1):
    """
    This function generates a new variable (new_gen_variable) in a dataframe (df) with an original value (new_original_value). 
    The values of the new variable are replaced with different values (range_new_gen) on a loop for the original list of variables 
    (list_ext_variables) depending on different conditions on other variables (value_originallist) already existing in the data frame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame to generate new variables in.
    new_gen_variable : str
        The name of the new generated variable.
    list_ext_variables : list of str
        The list of original variables in the DataFrame to apply conditions on.
    new_original_value : int, optional (default=4)
        The original value to initialize the new generated variable.
    range_new_gen : range object, optional (default=range(1,4))
        The range of values to replace the new generated variable with based on the conditions.
    value_originallist : int, optional (default=1)
        The condition for the original variables to meet for the new generated variable to be replaced with a value from the range.
    
    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the new generated variable added.
    """
    
    df[new_gen_variable] = new_original_value 
    for col, i in zip(list_ext_variables, range_new_gen):
         df.loc[df[col]==value_originallist, new_gen_variable]=i 
    return df


new_gen_variable_m1 = 'code'
list_ext_variables_m1 = ['jewish_inst', 'jewish_inst_one_block_away_1', 'cuad2']




def _gen_var_double_listed_mon(df, list_gen_var, list_ori_var, fixed_var):
    """
    This function generates new variables (columns) listed on a list (list_gen_var) in a dataframe (df), using a multiplication rule by a fixed variable 
    (fixed_var) that already exists in the data frame. The values for the new variables are calculated using an existing list of variables (list_ori_var)
    already present in the dataframe (df).
    
    Args:
    - df (pandas.DataFrame): the dataframe where the new variables will be created
    - list_gen_var (list): a list of strings representing the names of the new variables to be generated
    - list_ori_var (list): a list of strings representing the names of the existing variables used to calculate the values of the new variables
    - fixed_var (str): the name of an existing variable in the dataframe used to multiply the values of the original variables
    
    Returns:
    - df (pandas.DataFrame): the input dataframe with new columns added corresponding to the generated variables
    """
    for col1, col2 in zip(list_gen_var, list_ori_var):
        df[col1] = df[col2]*df[fixed_var] 
    return df

list_gen_var_m1 = ["jewish_inst_p", "jewish_inst_one_block_away_1_p"]
list_ori_var_m1 = ["jewish_inst",  "jewish_inst_one_block_away_1"]
fixed_var_m1 = 'post'

    
    


def _gen_rep_var_various_cond_equality_listedvalues_mon(df, NEW_var, ORI_var, list_a=[72,73], list_b=[7.2,7.3]):
    """
    This function generates a new variable (NEW_var) with an original value (ORI_var) and then replaces it based on a condition 
    on an existing variable (list_a) in a dataframe (df) using elements of a list (list_b) to replace the value.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The dataframe in which the variable is being generated and replaced.
    NEW_var : str
        The name of the new variable to be generated.
    ORI_var : str
        The name of the original variable.
    list_a : list, optional
        A list of values that will be used to determine which rows of the original variable to replace. Default is [72,73].
    list_b : list, optional
        A list of replacement values to use for each value in list_a. Default is [7.2,7.3].
        
    Returns:
    --------
    df : pandas.DataFrame
        The updated dataframe with the new variable and replaced values.
    """
    df[NEW_var] = df[ORI_var]
    for i,j in zip(list_a, list_b):
        df.loc[df[ORI_var]==i, NEW_var]=j
    return df


NEW_var_m1 = 'othermonth1'
ORI_var_m1 = 'month'
    




def _sort_mon(df, list_sort):
    """
    This function sorts the values of a dataframe (df) given a list of values to be used for sorting (list_sort).
    
    Parameters:
    -----------
    df : pandas DataFrame
        The DataFrame to be sorted.
    list_sort : list
        A list of column names or values that will be used to sort the DataFrame.
    
    Returns:
    --------
    pandas DataFrame
        The sorted DataFrame.
    """
    df = df.sort_values(list_sort)
    return df

list_sort_m1 = ['observ', 'month']





def _gen_rep_total_thefts2_mon(df, var_complex_cond, cond1=72, cond2=73):
    """This funtion is generating a a new variable by observation "total_thefts2" in a dataframe (df) 
    Args:
    df (pandas DataFrame): The input dataframe.
    var_complex_cond (str): The name of the column which contains the condition for selecting the cumulative sum.
    cond1 (int or float): The first value used for selecting the cumulative sum (default=72).
    cond2 (int or float): The second value used for selecting the cumulative sum (default=73).

    Returns:
    pandas DataFrame: The input dataframe with a new column "total_thefts2" added, which contains the calculated values based on the given condition."""
    df = df.assign(total_thefts2=pd.Series())
    df['total_thefts2'] = df['total_thefts2'].tolist()
    for i in range(1, len(df)):  
        if df[var_complex_cond].iloc[i] == cond1 or df[var_complex_cond].iloc[i] == cond2:
            df['total_thefts2'].loc[i] == df['total_thefts'].iloc[i].cumsum()
    df.loc[(df[var_complex_cond] != cond1) & (df[var_complex_cond] != cond2), 'total_thefts2'] = df['total_thefts']
    df['total_thefts2'] = pd.Series(df['total_thefts2'])
    return df

var_complex_cond_m1 = 'month'
    


def _df_to_csv_mon(df, location):
    """
    Saves a pandas DataFrame to a CSV file at the specified location.

    Args:
        df (pandas DataFrame): The DataFrame to be saved.
        location (str): The file path and name for the CSV file, including the ".csv" extension.

    Returns:
        None
    """
    df.to_csv(location)

location_m1 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel.csv'

    

#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_1(df, 
                   var_cond_ext,
                   new_var, var1, var_sub,
                   var_gen, cond_var,
                   var_cond_ext2,
                   ori_variables, fixed_variable,
                   new_gen_variable, list_ext_variables,
                   list_gen_var, list_ori_var, fixed_var,
                   NEW_var, ORI_var,
                   list_sort,
                   var_complex_cond,
                   location):
    df = _clean_column_names_mon(df)
    df = _gen_rep_var_fixed_extension_mon(df, var_cond_ext)
    df = _gen_var_difference_mon(df, new_var, var1, var_sub)
    df = _gen_rep_var_single_cond_biggerthan_mon(df, var_gen, cond_var)
    df = _gen_rep_var_fixed_extension_mon2(df, var_cond_ext2)
    df = _gen_var_cond_list_similar_mon(df, ori_variables, fixed_variable)
    df = _gen_rep_var_various_cond_equality_mon(df, new_gen_variable, list_ext_variables)
    df = _gen_var_double_listed_mon(df, list_gen_var, list_ori_var, fixed_var)
    df = _gen_rep_var_various_cond_equality_listedvalues_mon(df, NEW_var, ORI_var)
    df = _sort_mon(df, list_sort)
    df = _gen_rep_total_thefts2_mon(df, var_complex_cond)
    _df_to_csv_mon(df, location)


### FUNCTION ###

MonthlyPanel = monthlypanel_1(df=df_m1, 
                              var_cond_ext=var_cond_ext_m1,
                              new_var=new_var_m1, var1=var1_m1, var_sub=var_sub_m1,
                              var_gen=var_gen_m1, cond_var=cond_var_m1,
                              var_cond_ext2=var_cond_ext2_m1,
                              ori_variables=ori_variables_m1, fixed_variable=fixed_variable_m1,
                              new_gen_variable=new_gen_variable_m1, list_ext_variables=list_ext_variables_m1,
                              list_gen_var=list_gen_var_m1, list_ori_var=list_ori_var_m1, fixed_var=fixed_var_m1,
                              NEW_var=NEW_var_m1, ORI_var=ORI_var_m1,
                              list_sort=list_sort_m1, 
                              var_complex_cond=var_complex_cond_m1,
                              location=location_m1)

   

############################################## PART 2 ########################################################################################################################

def _reading_data_csv(location_origin):
    """
    Reads a CSV file located at the specified `location_origin` using pandas.

    Args:
    - location_origin (str): Path to the CSV file.

    Returns:
    - pandas.DataFrame: A pandas DataFrame containing the data read from the CSV file.
    """
    df = pd.read_csv(location_origin)
    return df

location_origin_m2 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel.csv'

   
    
    
def _drop_if_simple_mon2(df, var_drop, drop1=72, drop2=73):    
    """    Drops rows from a dataframe based on two conditions.

    Args:
    - df (pandas.DataFrame): The input dataframe.
    - var_drop (str): The name of the column that will be used to apply the conditions.
    - drop1 (int): The first value of `var_drop` for which rows will be dropped. Default value is 72.
    - drop2 (int): The second value of `var_drop` for which rows will be dropped. Default value is 73.

    Returns:
    - pandas.DataFrame: A new dataframe with the specified rows dropped.
    """    
    df.drop(df.loc[(df[var_drop]==drop1) | (df[var_drop]==drop2)].index, inplace=True)
    return df

var_drop_m2 = 'month'




def _gen_rep_various_cond_mon2(df, new_var_v_cond, ori_var_v_cond, var_con_v_cond, con_v_cond1=7, con_v_cond2=5, con_v_cond3=8, con_v_cond4=10, con_v_cond5=12, multiple1_v_cond=(30/17), multiple2_v_cond=(30/31)):
    """    
    Generate a new variable in a dataframe based on the values of another existing variable and a condition on a variable in the dataframe.

    Args:
    - df (pandas.DataFrame): The input dataframe.
    - new_var_v_cond (str): The name of the new variable to be generated in the dataframe.
    - ori_var_v_cond (str): The name of the existing variable that will be used to generate the new variable.
    - var_con_v_cond (str): The name of the variable that will be used to apply the condition on the dataframe.
    - con_v_cond1 (int): The first value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 7.
    - con_v_cond2 (int): The second value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 5.
    - con_v_cond3 (int): The third value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 8.
    - con_v_cond4 (int): The fourth value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 10.
    - con_v_cond5 (int): The fifth value of `var_con_v_cond` that changes the condition and affects the generated variable. Default value is 12.
    - multiple1_v_cond (float): The scaling factor for the generated variable when `var_con_v_cond` equals `con_v_cond1`. Default value is 30/17.
    - multiple2_v_cond (float): The scaling factor for the generated variable when `var_con_v_cond` equals any of `con_v_cond2`, `con_v_cond3`, `con_v_cond4`, or `con_v_cond5`. Default value is 30/31.

    Returns:
    - pandas.DataFrame: A new dataframe with the new variable added and values of the new variable generated based on the specified conditions.
    """ 
    
    df[new_var_v_cond] = df[ori_var_v_cond] 
    df.loc[df[var_con_v_cond]==con_v_cond1, new_var_v_cond]= df[ori_var_v_cond]*multiple1_v_cond 
    df.loc[(df[var_con_v_cond]==con_v_cond2) | (df[var_con_v_cond]==con_v_cond3) | (df[var_con_v_cond]==con_v_cond4) | (df[var_con_v_cond]==con_v_cond5), new_var_v_cond]= df[ori_var_v_cond]*multiple2_v_cond # replace
    return df 

new_var_v_cond_m2 = 'total_thefts_c'
ori_var_v_cond_m2 = 'total_thefts'
var_con_v_cond_m2 = 'month'






def _genNA_rep_two_cond_mon2(df, list_for_NA, var_con_NA, fixed_var_NA, NA_value=7):
    """Generates a set of NA variables in a dataframe (df) based on a list of variable names (list_for_NA). 
    The value of some of the variables on the list are replaced with values of an original variable in the 
    dataframe (fixed_var_NA) based on a list of conditions over a variable (var_con_NA). The conditions check 
    whether this variable has a value higher or lower than a fixed value (NA_value).

    Args:
        df (pandas.DataFrame): The input dataframe.
        list_for_NA (list): A list of variable names to generate NAs.
        var_con_NA (str): The name of the variable that is used to apply conditions on whether to replace values or not.
        fixed_var_NA (str): The name of the variable used to replace the values in the list_for_NA.
        NA_value (int): The fixed value to compare the variable var_con_NA against. Default is 7.

    Returns:
        pandas.DataFrame: The dataframe with new variables generated with NAs and some variables replaced
        based on the specified conditions.
    """ 
    for col in list_for_NA:
        df[col] = pd.NA  
    df.loc[df[var_con_NA]<NA_value+1, list_for_NA[0]]=df[fixed_var_NA]
    df.loc[df[var_con_NA]>NA_value, list_for_NA[1]]=df[fixed_var_NA]
    return df

list_for_NA_m2 = ['prethefts', 'posthefts', 'theftscoll']
var_con_NA_m2 = 'month'
fixed_var_NA_m2 = 'total_thefts'

    



def _sort_mon2(df, list_sort):
    """Sorts the rows of a dataframe (df) based on the values of the columns specified in the list (list_sort).

    Args:
        df (pandas.DataFrame): The input dataframe.
        list_sort (list): A list of column names to use for sorting the dataframe.

    Returns:
        pandas.DataFrame: The sorted dataframe.
    """
    df = df.sort_values(list_sort)
    return df

list_sort_m2 = ['observ', 'month']






def _egen_rep_mon2(df, new_egen_var, by_var, var_egen_sup, cond_ege_var, ege_var_change, cond_ege_val=4, ege_scale_factor=4):
    """Generates a new variable (new_egen_var) in a dataframe (df) based on the sum of an existing variable (var_egen_sup) 
    grouped by a by variable (by_var). Then, replaces the values of an existing variable (ege_var_change) in the dataframe
    with the new variable scaled by a given factor (ege_scale_factor) if an existing variable (cond_ege_var) meets a certain 
    condition (cond_ege_val).

    Args:
        df (pandas.DataFrame): The input dataframe.
        new_egen_var (str): The name of the new variable to be generated.
        by_var (str): The name of the by variable to group the sum of an existing variable.
        var_egen_sup (str): The name of the existing variable to take the sum of.
        cond_ege_var (str): The name of the existing variable to check the condition for replacement.
        ege_var_change (str): The name of the existing variable to be replaced.
        cond_ege_val (int, optional): The condition value for replacement. Defaults to 4.
        ege_scale_factor (int, optional): The scaling factor for the new variable. Defaults to 4.

    Returns:
        pandas.DataFrame: The modified dataframe.
    """
    df[new_egen_var] = df.groupby(by_var)[var_egen_sup].transform('sum')
    df.loc[df[cond_ege_var]==cond_ege_val, ege_var_change]=df[new_egen_var]/ege_scale_factor
    return df

new_egen_var_m2 = 'totalpre'
by_var_m2 = 'observ' 
var_egen_sup_m2 = 'prethefts'
cond_ege_var_m2 = 'month'
ege_var_change_m2 = 'theftscoll'







def _egen_rep2_mon2(df, new_egen_var2, by_var2, var_egen_sup2, cond_ege_var2, ege_var_change2, cond_ege_val2=8, ege_scale_factor2=5):
    """Generates a new variable (new_egen_var) in a dataframe (df) based on the sum of an existing variable (var_egen_sup) 
    grouped by a by variable (by_var). Then, replaces the values of an existing variable (ege_var_change) in the dataframe
    with the new variable scaled by a given factor (ege_scale_factor) if an existing variable (cond_ege_var) meets a certain 
    condition (cond_ege_val).

    Args:
        df (pandas.DataFrame): The input dataframe.
        new_egen_var2 (str): The name of the new variable to be generated.
        by_var2 (str): The name of the by variable to group the sum of an existing variable.
        var_egen_sup2 (str): The name of the existing variable to take the sum of.
        cond_ege_var2 (str): The name of the existing variable to check the condition for replacement.
        ege_var_change2 (str): The name of the existing variable to be replaced.
        cond_ege_val2 (int, optional): The condition value for replacement. Defaults to 8.
        ege_scale_factor2 (int, optional): The scaling factor for the new variable. Defaults to 5.

    Returns:
        pandas.DataFrame: The modified dataframe.
    """
    df[new_egen_var2] = df.groupby(by_var2)[var_egen_sup2].transform('sum')
    df.loc[df[cond_ege_var2]==cond_ege_val2, ege_var_change2]=df[new_egen_var2]/ege_scale_factor2
    return df

new_egen_var2_m2 = 'totalpos'
by_var2_m2 = 'observ' 
var_egen_sup2_m2 = 'posthefts'
cond_ege_var2_m2 = 'month'
ege_var_change2_m2 = 'theftscoll'






def _gen_simplebased_mon2(df, new_generated_var, existing_var, scalar_gen=4):
    """Generate a new variable (new_generated_var) in a Pandas DataFrame (df) based on an existing variable (existing_var) 
    in the same dataframe multiplied by a scalar value (scalar_gen).

    Args:
        df (Pandas DataFrame): The input dataframe containing the existing variable and where the new variable will be created
        new_generated_var (str): The name of the new variable to be created
        existing_var (str): The name of the existing variable in the dataframe to use as the basis for the new variable
        scalar_gen (int or float, optional): The scalar value to multiply the existing variable with. Defaults to 4.

    Returns:
        Pandas DataFrame: The input dataframe with a new column (new_generated_var) added based on the existing variable
        multiplied by the scalar value.
    """
    df[new_generated_var] = df[existing_var] * scalar_gen
    return df
    
new_generated_var_m2 = 'total_thefts_q' 
existing_var_m2 = 'total_thefts'





def _gen_simple_mon2(df, new_gen_var_sim, value_sim=0.25):
    """Generate a new variable (new_gen_var_sim) in a dataframe (df) with a constant scalar value (value_sim).

    Args:
        df (pandas.DataFrame): The input dataframe.
        new_gen_var_sim (str): The name of the new variable to be generated.
        value_sim (float, optional): The scalar value to assign to the new variable. Defaults to 0.25.

    Returns:
        pandas.DataFrame: The input dataframe with the new variable added.
    """
    df[new_gen_var_sim] = value_sim
    return df

new_gen_var_sim_m2 = 'w'  





def _gen_simple2_mon2(df, new_gen_var_sim2, value_sim2=0):
    """Generate a new variable (new_gen_var_sim) in a dataframe (df) with a constant scalar value (value_sim).

    Args:
        df (pandas.DataFrame): The input dataframe.
        new_gen_var_sim3 (str): The name of the new variable to be generated.
        value_sim2 (float, optional): The scalar value to assign to the new variable. Defaults to 0.

    Returns:
        pandas.DataFrame: The input dataframe with the new variable added.
    """
    df[new_gen_var_sim2] = value_sim2
    return df

new_gen_var_sim2_m2 = 'n_neighborhood'  





def _complex_gen_rep_mon2(df, list_names_complex, var_rep_cond_complex, var_rep_complex, gen_var_complex, var_cond_complex, list_names_complexb, scale_complex=1000, range_complex=range(1,4), value_last_complex=1):
    """
    This function replaces the values of a variable (var_rep_complex) in a dataframe (df) by using a condition on another 
    variable already existent in the dataframe (var_rep_cond_complex). The condition is based on a list of names 
    (list_names_complex) and the replace value is based on a range (range_complex).

    Then a new variable is generated (gen_var_complex) based on two other variables summed in the dataframe 
    (var_cond_complex, var_rep_complex) where the second one is multiplied by a scalar factor (scale_complex).

    Finally, two variables are generated based on a list of names (list_names_complex and list_names_complexb). The values of 
    the new generated variables are replaced based on a condition on an existing variable in the dataframe 
    (var_rep_cond_complex), and in the end, the variables receive a final value (value_last_complex).

    Parameters:
    -----------
    df : pandas DataFrame
        The dataframe that contains the variables to be manipulated.
    list_names_complex : list
        A list of strings containing the names to be used in the replacement of var_rep_complex based on var_rep_cond_complex.
    var_rep_cond_complex : str
        The name of the existing variable used as a condition to replace the values in var_rep_complex.
    var_rep_complex : str
        The name of the variable whose values will be replaced.
    gen_var_complex : str
        The name of the new generated variable to be added to the dataframe.
    var_cond_complex : str
        The name of the variable to be added to gen_var_complex.
    list_names_complexb : list
        A list of strings containing the names to be used in the replacement of the new generated variables.
    scale_complex : int, optional
        The scalar factor to be applied to var_rep_complex when generating gen_var_complex. Default is 1000.
    range_complex : range, optional
        The range of values to be used in the replacement of var_rep_complex based on var_rep_cond_complex. Default is range(1,4).
    value_last_complex : int, optional
        The final value to be applied to the new generated variables. Default is 1.

    Returns:
    --------
    df : pandas DataFrame
        The dataframe with the new generated variables and the modified values of the existing ones.
    """
    for col, i in zip(list_names_complex, range_complex):
        df.loc[df[var_rep_cond_complex]==col, var_rep_complex]=i 
    df[gen_var_complex] = df[var_cond_complex] + scale_complex*df[var_rep_complex]
    for col1, col2 in zip(list_names_complexb, list_names_complex):
        df[col1] = 0 
        df.loc[df[var_rep_cond_complex]==col2, col1]= value_last_complex
    return df

list_names_complex_m2 = ['Belgrano', 'Once', 'V. Crespo']
var_rep_cond_complex_m2 = 'neighborhood'
var_rep_complex_m2 = 'n_neighborhood'
gen_var_complex_m2 = 'code2'
var_cond_complex_m2 = 'month'
list_names_complexb_m2 = ['belgrano', 'once', 'vcrespo']





def _gen_rep_simple_mon2(df, var_gen_simple, var_cond_simple, original_val_simple=0, cond_simple=4, value_final_simple=1):
    """"
    Generates a new variable in a dataframe and replaces its values based on a condition.

    Args:
    - df: input pandas DataFrame
    - var_gen_simple (str): name of the new variable to be generated
    - var_cond_simple (str): name of the existing variable used as a condition to replace values in the new variable
    - original_val_simple (float, optional): original value of the new variable (default is 0)
    - cond_simple (int, optional): condition to be checked against the values in var_cond_simple (default is 4)
    - value_final_simple (float, optional): final value to be assigned to var_gen_simple if the condition is met (default is 1)

    Returns:
    - df (pandas DataFrame): the input DataFrame with the new variable and replaced values
    """
    df[var_gen_simple] = original_val_simple
    df.loc[df[var_cond_simple]==cond_simple, var_gen_simple]=value_final_simple
    return df

var_gen_simple_m2 = 'month4'
var_cond_simple_m2 = 'month'

    




def _gen_based_variouslists_mon2(df, list_names_place, list_names_month, list_names_variouslists):
    """
    This function is firstly extending two lists which in turn will be used to create a set of variables (list_names_place) based on the values of two different 
    other lists (list_names_month,  list_names_variouslists_m2) multiplied by each other
    
    Args:
    - df: Pandas DataFrame to generate the variables on.
    - list_names_place: List of variable names to be created.
    - list_names_month: List of month names.
    - list_names_variouslists: List of variable names to be multiplied to generate new variables.

    Returns:
    - df: Pandas DataFrame with the newly created variables.
    """
    
    list_names_place.extend([f"mbelg{i}" for i in ["may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]])
    list_names_place.extend([f"monce{i}" for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]])
    list_names_place.extend([f"mvcre{i}" for i in ["apr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dec"]])
    list_names_month.extend([f"month{i}" for i in range(5,13)])

    """ This is making a loop over the three different elements of list_names_variouslists_m2. Ity is usimng an index to go through thje 27 elements in list_names_place
    and it is using a second loop to go over the elemensr of list_names_month. """
    for i in range(3):
        start_idx = i * 9
        for j in range(9):
            col1 = f"{list_names_place[start_idx + j]}"
            col2 = list_names_variouslists[i]
            col3 = list_names_month[j]
            df[col1] = df[col2] * df[col3]         
    return df
                     

list_names_place_m2 = ["mbelgapr"]
list_names_month_m2 = ["month4"]      
list_names_variouslists_m2 = ['belgrano', 'once', 'vcrespo']
  




def _df_to_csv_mon2(df, location):
    """
    This function saves the input dataframe (df) as a CSV file at the specified location (location).

    Args:
    df: pandas.DataFrame
        The DataFrame to be saved as a CSV file.
    location: str
        The file path including the file name and extension, where the CSV file will be saved.
        
    Returns:
    None
    """
    df.to_csv(location)
    
location_m2 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel2.csv'



#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_2(location_origin,
                   var_drop,
                   new_var_v_cond, ori_var_v_cond, var_con_v_cond,
                   list_for_NA, var_con_NA, fixed_var_NA,
                   list_sort,
                   new_egen_var, by_var, var_egen_sup, cond_ege_var, ege_var_change,
                   new_egen_var2, by_var2, var_egen_sup2, cond_ege_var2, ege_var_change2,
                   new_generated_var, existing_var,
                   new_gen_var_sim, 
                   new_gen_var_sim2,
                   list_names_complex, var_rep_cond_complex, var_rep_complex, gen_var_complex, var_cond_complex, list_names_complexb,
                   var_gen_simple, var_cond_simple,
                   list_names_place, list_names_month, list_names_variouslists,
                   location):
    df = _reading_data_csv(location_origin)
    df = _drop_if_simple_mon2(df, var_drop)
    df = _gen_rep_various_cond_mon2(df, new_var_v_cond, ori_var_v_cond, var_con_v_cond)
    df = _genNA_rep_two_cond_mon2(df, list_for_NA, var_con_NA, fixed_var_NA)
    df = _sort_mon2(df, list_sort)
    df = _egen_rep_mon2(df, new_egen_var, by_var, var_egen_sup, cond_ege_var, ege_var_change)
    df = _egen_rep2_mon2(df, new_egen_var2, by_var2, var_egen_sup2, cond_ege_var2, ege_var_change2)
    df = _gen_simplebased_mon2(df, new_generated_var, existing_var)
    df = _gen_simple_mon2(df, new_gen_var_sim)
    df = _gen_simple2_mon2(df, new_gen_var_sim2)
    df = _complex_gen_rep_mon2(df, list_names_complex, var_rep_cond_complex, var_rep_complex, gen_var_complex, var_cond_complex, list_names_complexb)
    df = _gen_rep_simple_mon2(df, var_gen_simple, var_cond_simple)
    df = _gen_based_variouslists_mon2(df, list_names_place, list_names_month, list_names_variouslists)
    _df_to_csv_mon2(df, location) 
    
### FUNCTION ###

MonthlyPanel2 = monthlypanel_2(location_origin=location_origin_m2, 
                   var_drop=var_drop_m2,
                   new_var_v_cond=new_var_v_cond_m2, ori_var_v_cond=ori_var_v_cond_m2, var_con_v_cond=var_con_v_cond_m2,
                   list_for_NA=list_for_NA_m2, var_con_NA=var_con_NA_m2, fixed_var_NA=fixed_var_NA_m2,
                   list_sort=list_sort_m2,
                   new_egen_var=new_egen_var_m2, by_var=by_var_m2, var_egen_sup=var_egen_sup_m2, cond_ege_var=cond_ege_var_m2, ege_var_change=ege_var_change_m2,
                   new_egen_var2=new_egen_var2_m2, by_var2=by_var2_m2, var_egen_sup2=var_egen_sup2_m2, cond_ege_var2=cond_ege_var2_m2, ege_var_change2=ege_var_change2_m2,
                   new_generated_var=new_generated_var_m2, existing_var=existing_var_m2,
                   new_gen_var_sim=new_gen_var_sim_m2,
                   new_gen_var_sim2= new_gen_var_sim2_m2,
                   list_names_complex=list_names_complex_m2, var_rep_cond_complex=var_rep_cond_complex_m2, var_rep_complex=var_rep_complex_m2, gen_var_complex=gen_var_complex_m2, var_cond_complex=var_cond_complex_m2, list_names_complexb=list_names_complexb_m2,
                   var_gen_simple=var_gen_simple_m2, var_cond_simple=var_cond_simple_m2,
                   list_names_place=list_names_place_m2, list_names_month=list_names_month_m2, list_names_variouslists=list_names_variouslists_m2,
                   location=location_m2)



############################################## PART 3 ########################################################################################################################


def _reading_data_csv(location_origin):
    """
    Reads a CSV file located at the specified `location_origin` using pandas.

    Args:
    - location_origin (str): Path to the CSV file.

    Returns:
    - pandas.DataFrame: A pandas DataFrame containing the data read from the CSV file.
    """
    df = pd.read_csv(location_origin)
    return df

location_origin_m3 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel2.csv'
    





def _single_drop(df, column_to_drop):
    """
    This function drops a single column (column_to_drop) from a pandas dataframe (df) and returns a new dataframe
    with the dropped column.
    
    Inputs:
    - df: pandas dataframe to drop a column from
    - column_to_drop: string representing the name of the column to be dropped
    
    Output:
    - df: pandas dataframe with the specified column removed
    """

    df = df.drop(columns=column_to_drop)
    return df

column_to_drop_m3 = 'month4'






def _gen_rep_3cond_mon3(df, gen_var_3cond, col1_3cond, col2_3cond, col3_3cond, initial_val_3cond=0, global_replace_val_3cond=1):
    """
    This function is generating a variable (gen_var_3cond) in a dataframe (df) with an initial value initial_val_3cond.
    Then it is replacing the value of this variable if any of three condition on three variables are met (col1_3cond, col2_3cond, col3_3cond)
    The values they need to meet and the value of the new variable in the end is the same (global_replace_val_3cond)
    
    Args:
    - df: input pandas dataframe.
    - gen_var_3cond: string representing the name of the new variable to generate.
    - col1_3cond: string representing the name of the first column to consider for the conditions.
    - col2_3cond: string representing the name of the second column to consider for the conditions.
    - col3_3cond: string representing the name of the third column to consider for the conditions.
    - initial_val_3cond: optional, integer representing the initial value for the new variable (default=0).
    - global_replace_val_3cond: optional, integer representing the value to use when replacing the new variable (default=1).

    Returns:
    - df: pandas dataframe with the new variable generated and potentially replaced.
    """
    df[gen_var_3cond] = initial_val_3cond 
    df.loc[(df[col1_3cond]==global_replace_val_3cond) | (df[col2_3cond]==global_replace_val_3cond) | (df[col3_3cond]==global_replace_val_3cond), gen_var_3cond]=global_replace_val_3cond
    return df

gen_var_3cond_m3 = 'all_locations'
col1_3cond_m3 = 'public_building_or_embassy'
col2_3cond_m3 = 'gas_station'
col3_3cond_m3 = 'bank'







def _gen_multiplevariables_listbased(df, list_values, list_names_3_variables, list_names_data3_general):
    """
    This function is list based. Given some list entries (list_value1, list_value2, list_value3, list_value4, list_names_3_variables, list_names_data3_general), 
    various variables are created in a data frame (df).
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The dataframe to which variables will be added.
    list_values : list
        A list of values for each variable to be created. Each element of the list should be a list of length 6 containing
        three names for variables to be created using multiplication of columns from the dataframe, and three names for 
        variables to be created using the complement (1-x) of one of the multiplication variables and another column from
        the dataframe.
    list_names_3_variables : list
        A list of names of columns from the dataframe to be used for the first three variables of each element in list_values.
    list_names_data3_general : list
        A list of names of columns from the dataframe to be used for the last three variables of each element in list_values.

    Returns:
    --------
    pandas.DataFrame
        The input dataframe with additional columns added based on the values in the input lists.
    
    """
    for i, values in enumerate(list_values):
        for j in range(3):
            df[values[j]] = df[list_names_3_variables[i]] * df[list_names_data3_general[j]]
        for j in range(3, 6):
            df[values[j]] = (1-df[list_names_3_variables[i]]) * df[list_names_data3_general[j]]        
    return df

list_value1_m3 = ['public_building_or_embassy_p', 'public_building_or_embassy_1_p', 'public_building_or_embassy_cuad2p', 'n_public_building_or_embassy_p', 'n_public_building_or_embassy_1_p', 'n_public_building_or_embassy_cuad2p']
list_value2_m3 = ['gas_station_p', 'gas_station_1_p', 'gas_station_cuad2p', 'n_gas_station_p', 'n_gas_station_1_p', 'n_gas_station_cuad2p']
list_value3_m3 = ['bank_p', 'bank_1_p', 'bank_cuad2p', 'n_bank_p', 'n_bank_1_p', 'n_bank_cuad2p']
list_value4_m3 = ['all_locations_p', 'all_locations_1_p', 'all_locations_cuad2p', 'n_all_locations_p', 'n_all_locations_1_p', 'n_all_locations_cuad2p']
list_names_3_variables_m3 = ['public_building_or_embassy', 'gas_station', 'bank', 'all_locations']
list_names_data3_general_m3 = ['jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p', 'jewish_inst_p', 'jewish_inst_one_block_away_1_p', 'cuad2p'] 






def _df_to_csv_mon3(df, location):
    """
    This function saves the input dataframe (df) as a CSV file at the specified location (location).

    Args:
    df: pandas.DataFrame
        The DataFrame to be saved as a CSV file.
    location: str
        The file path including the file name and extension, where the CSV file will be saved.
        
    Returns:
    None
    """
    df.to_csv(location)
    
location_m3 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel3.csv'



#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_3(location_origin, 
                   column_to_drop,
                   gen_var_3cond, col1_3cond, col2_3cond, col3_3cond,
                   list_values, list_names_3_variables, list_names_data3_general,
                   location):
    df = _reading_data_csv(location_origin)
    df = _single_drop(df, column_to_drop)
    df = _gen_rep_3cond_mon3(df, gen_var_3cond, col1_3cond, col2_3cond, col3_3cond)
    df = _gen_multiplevariables_listbased(df, list_values, list_names_3_variables, list_names_data3_general)
    _df_to_csv_mon3(df, location)

### FUNCTION ###

MonthlyPanel3 = monthlypanel_3(location_origin=location_origin_m3, 
                               column_to_drop=column_to_drop_m3,
                               gen_var_3cond=gen_var_3cond_m3, col1_3cond=col1_3cond_m3, col2_3cond=col2_3cond_m3, col3_3cond=col3_3cond_m3,
                               list_values=[list_value1_m3, list_value2_m3, list_value3_m3, list_value4_m3], list_names_3_variables=list_names_3_variables_m3, list_names_data3_general=list_names_data3_general_m3,
                               location=location_m3) 


############################################## PART 4 ########################################################################################################################
    
# drop all variables created before
#MonthlyPanel4 = MonthlyPanel3.drop(columns=[list_names_data3_1, list_names_data3_2, list_names_data3_3, list_names_data3_4], inplace=True)

######################################################################################################################################################################

MonthlyPanel_new, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/MonthlyPanel.dta')


def _clean_column_names_mon4(df):
    """
    Clean the column names of a given dataframe (df) to make them more readable.
    
    Args:
    - df (pandas.DataFrame): The dataframe with unclean column names.
    
    Returns:
    - df (pandas.DataFrame): The same dataframe with cleaned column names.
    """  
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

df_m4 = MonthlyPanel_new





def _gen_var_difference_mon4(df, new_var_new, var1_new, var_sub_new):
    """Generate a new variable (new_var_new) in a dataframe (df) by subtracting two existing columns (var1_new and var_sub_new).
    
    Args:
    df (pandas dataframe): The dataframe to add the new variable to.
    new_var_new (str): The name of the new variable to be created.
    var1_new (str): The name of the first column to be used for subtraction.
    var_sub_new (str): The name of the second column to be used for subtraction.
    
    Returns:
    pandas dataframe: The updated dataframe with the new variable.
    """
    df[new_var_new] = df[var1_new] - df[var_sub_new]
    return df

new_var_new_m4 = 'jewish_inst_one_block_away_1'
var1_new_m4 = 'jewish_inst_one_block_away'
var_sub_new_m4 = 'jewish_inst'






def _gen_variouslisted_var_mon4(df, list_various_gen, original_val_various_list=0):
    """
    Generates a set of new variables specified by a list (list_various_gen) in a dataframe (df), 
    and assigns an initial value (original_val_various_list) to each variable.

    Args:
    - df: Pandas DataFrame.
    - list_various_gen: list of new variable names to generate.
    - original_val_various_list: scalar or list of initial values to assign to the new variables. Default value set to 0.

    Returns:
    - df: Pandas DataFrame with the new variables added.
    """
    df[[col for col in list_various_gen]] = original_val_various_list
    return df

list_various_gen_m4 = ['cuad2', 'month5', 'month6', 'month7']

    





def _rep_simple_mon4(df, cond_var_simple, var_simple_rep, cond_val_simple=2, val_assigned_simple=1):
    """
    Replace values in a single variable of a DataFrame (df) based on a condition. 
    
    Args:
    - df (pandas.DataFrame): The input DataFrame.
    - cond_var_simple (str): The name of the variable in the DataFrame on which the conditional check is performed.
    - var_simple_rep (str): The name of the variable in the DataFrame that will be replaced if the condition is met.
    - cond_val_simple (int, optional): The value that the condition variable must meet to proceed with the replacement. Default value is 2.
    - val_assigned_simple (int, optional): The value to be assigned to the replacement variable if the condition is met. Default value is 1.
    
    Returns:
    - pandas.DataFrame: The modified DataFrame with the variable values replaced according to the condition.
    """
    df.loc[df[cond_var_simple]==cond_val_simple, var_simple_rep] = val_assigned_simple
    return df

cond_var_simple_m4 = 'distance_to_jewish_inst'
var_simple_rep_m4 = 'cuad2'

    




def _rep_various_mon4(df, cond_var_simple_v, range_replace=range (5,8), val_assigned_various=1):
    """
    Replace multiple variables in a dataframe (df) by looping through a fixed range (range_replace) based on a condition
    that should be met by another variable in the dataframe (cond_var_simple_v). If the condition is met, the value assigned 
    to these variables is also fixed (val_assigned_various).

    Parameters:
    -----------
    df: pandas DataFrame
        The dataframe where variables need to be replaced.

    cond_var_simple_v: str
        The name of the column in df that should meet the condition.

    range_replace: range object, optional (default: range(5, 8))
        A range object that represents the fixed range to loop through.

    val_assigned_various: int, optional (default: 1)
        The value that needs to be assigned to the variables if the condition is met.

    Returns:
    --------
    pandas DataFrame
        A new pandas DataFrame with replaced variables based on the given condition and fixed range.
    """
    for i in range_replace:
        df.loc[df[cond_var_simple_v]==i, f'month{i}'] = val_assigned_various
    return df

cond_var_simple_v_m4 = 'month'

    




def _drop_conditional(df, var_drop_cond, drop_condition_list=[72, 73, 8, 9, 10, 11, 12]):
    """This function drops rows from a dataframe (df) for which a specific variable (var_drop_cond) meets a condition given by a list of values (drop_condition_list).
    
    Args:
    - df: a pandas dataframe.
    - var_drop_cond (str): the variable/column name in the dataframe to be used for conditional dropping.
    - drop_condition_list (list): a list of values for the conditional dropping. Default is [72, 73, 8, 9, 10, 11, 12].
    
    Returns:
    - df: the updated pandas dataframe without the rows that meet the specified condition.
    """ 
    df.drop(df[df[var_drop_cond].isin(drop_condition_list)].index, inplace=True)
    return df

var_drop_cond_m4 = 'month'





    
def _gen_rep_var_various_cond_biggerthan_mon4(df, list_gen_var_cond, cond_var_cond, original_value_var_cond=0, range_var_cond=range(4,7), final_value_var_cond=1):
    """
    Generate and replace variables in a dataframe based on a loop range and a condition on another column.

    Args:
        df (pandas.DataFrame): The input dataframe.
        list_gen_var_cond (list): A list of variables to generate.
        cond_var_cond (str): The name of the column containing the condition.
        original_value_var_cond (int, optional): The original value to assign to the generated variables. Defaults to 0.
        range_var_cond (range, optional): A range object for the loop to generate and replace variables. Defaults to range(4,7).
        final_value_var_cond (int, optional): The value to assign to the generated variables when the condition is met. Defaults to 1.

    Returns:
        pandas.DataFrame: A modified version of the input dataframe with the generated variables and the replacements.
    """
    for col in list_gen_var_cond:
        df[col] = original_value_var_cond
    for col, i in zip(list_gen_var_cond, range_var_cond):
        df.loc[df[cond_var_cond]>i, col] = final_value_var_cond
    return df

list_gen_var_cond_m4 = ['post1', 'post2', 'post3']
cond_var_cond_m4 = 'month'







def _gen_specificrule_list(df, list_var_gen_spec, list_var_ext_spec, list_new_var_spec, range_specific_loop=[0,3,6]):
    """
    This function is generating specific new variables (ist_var_gen_spec) in a dataframe (df) based on already existing set of variables in the data frame
    (list_var_ext_spec, list_new_var_spec) and this is done over a loop range(range_specific_loop)
    
    Inputs:

    df: a pandas DataFrame
    list_var_gen_spec: a list of new variable names to be generated in the DataFrame df
    list_var_ext_spec: a list of existing variable names in the DataFrame df to be used in the generation of new variables
    list_new_var_spec: a list of new variable names to be generated in the DataFrame df
    range_specific_loop: a list of integers representing the indices for generating the new variables
    
    Outputs:

    df: a pandas DataFrame with the newly generated variables based on specific rules
    """
    for i in range_specific_loop:
        for col1, col2, col3 in zip(list_var_gen_spec, list_var_ext_spec, list_new_var_spec[i:i+3]):
            df[col3] = df[col1] * df[col2]
    return df

list_var_gen_spec_m4 = ['jewish_inst', 'jewish_inst_one_block_away_1', 'cuad2'] 
list_var_ext_spec_m4 = ['post1', 'post2', 'post3']
list_new_var_spec_m4 = ['one_jewish_inst_1_p', 'one_jewish_inst_one_block_away_1_p', 'one_cuad2p', 'two_jewish_inst_1_p', 'two_jewish_inst_one_block_away_1_p', 'two_cuad2p', 'three_jewish_inst_1_p', 'three_jewish_inst_one_block_away_1_p', 'three_cuad2p']
    




def _dropna_mon4(df):
    """
    Parameters:
    df : pandas DataFrame
    The DataFrame to be cleaned from missing values.

    Returns:
    pandas DataFrame
    The cleaned DataFrame with no missing values.
    """
    df = df.dropna()
    return df





def _df_to_csv_mon4(df, location):
    """
    This function saves the input dataframe (df) as a CSV file at the specified location (location).

    Args:
    df: pandas.DataFrame
        The DataFrame to be saved as a CSV file.
    location: str
        The file path including the file name and extension, where the CSV file will be saved.
        
    Returns:
    None
    """
    df.to_csv(location)
    
location_m4 = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/MonthlyPanel_new.csv'


#### TRYING TO PUT EVERYTHING INSIDE A FUNCTION ####

def monthlypanel_4(df,
                   new_var_new, var1_new, var_sub_new,
                   list_various_gen,
                   cond_var_simple, var_simple_rep,
                   cond_var_simple_v,
                   var_drop_cond, 
                   list_gen_var_cond, cond_var_cond,
                   list_var_gen_spec, list_var_ext_spec, list_new_var_spec,
                   location):
    df = _clean_column_names_mon4(df)
    df = _gen_var_difference_mon4(df, new_var_new, var1_new, var_sub_new)
    df = _gen_variouslisted_var_mon4(df, list_various_gen)
    df = _rep_simple_mon4(df, cond_var_simple, var_simple_rep)
    df = _rep_various_mon4(df, cond_var_simple_v)
    df = _drop_conditional(df, var_drop_cond)
    df = _gen_rep_var_various_cond_biggerthan_mon4(df, list_gen_var_cond, cond_var_cond)
    df = _gen_specificrule_list(df, list_var_gen_spec, list_var_ext_spec, list_new_var_spec)
    df = _dropna_mon4(df)
    _df_to_csv_mon4(df, location)
    
### FUNCTION ###

MonthlyPanel_new = monthlypanel_4(df = MonthlyPanel_new,
                   new_var_new = new_var_new_m4, var1_new=var1_new_m4, var_sub_new=var_sub_new_m4,
                   list_various_gen=list_various_gen_m4,
                   cond_var_simple=cond_var_simple_m4, var_simple_rep=var_simple_rep_m4,
                   cond_var_simple_v=cond_var_simple_v_m4,
                   var_drop_cond=var_drop_cond_m4,
                   list_gen_var_cond=list_gen_var_cond_m4, cond_var_cond=cond_var_cond_m4,
                   list_var_gen_spec=list_var_gen_spec_m4, list_var_ext_spec=list_var_ext_spec_m4, list_new_var_spec=list_new_var_spec_m4,
                   location=location_m4)