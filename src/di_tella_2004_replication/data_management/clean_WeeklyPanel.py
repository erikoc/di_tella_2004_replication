"""Function(s) for cleaning the data set(s)."""
import numpy as np
import pandas as pd
import pyreadstat  as pyread

""" Weekly Panel """

WeeklyPanel, meta = pyread.read_dta('/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data/WeeklyPanel.dta')




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

df_we = WeeklyPanel




def _drop_variables_we(df, list_drop):
    """
    Drops columns from a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame from which to drop columns.
        list_drop (list): A list of column names to drop from the DataFrame.

    Returns:
        pandas.DataFrame: The input DataFrame with the specified columns dropped.

    """
    df.drop(columns=list_drop, inplace=True)
    return df

list_drop_we = ['street', 'street_nr', 'public_building_or_embassy', 'gas_station', 'bank']




    
def _gen_rep_variables_fixedextension_we(df, var_cond_ext, range_loop=range(1,40), original_value_var=0, final_value_var=1):    
    """
    Generates a set of variables based on a fixed extension, and replaces their values in a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame in which to generate and replace variables.
        var_cond_ext (str): The name of the variable in df used to condition the replacement.
        range_loop (range or iterable): An iterable with the values of the extension to use for variable names.
            Default is range(1, 40).
        original_value_var (int or any): The initial value to set to each generated variable.
            Default is 0.
        final_value_var (int or any): The final value to set to the variables that match the condition var_cond_ext==i.
            Default is 1.

    Returns:
        pandas.DataFrame: The input DataFrame with the generated variables replaced according to the condition.
    """ 
    for i in range_loop:
        list_names_ext = []
        list_names_ext.extend([f"week{i}"])  
        df[[col for col in list_names_ext]] = original_value_var # generate
    for i in range_loop:
        df.loc[df[var_cond_ext]==i, f"week{i}"]= final_value_var # replace
    return df

var_cond_ext_we = 'week'






def _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, var_fix, cond_fix=2, value_var_fix_ori=0, value_var_fix_fin=1): 
    """
    Generates a set of variables based on a fixed list, and replaces their values in a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame in which to generate and replace variables.
        list_fixed (list): A list of column names to generate.
        var_cond_fix (str): The name of the variable in df used to condition the replacement.
        var_fix (str): The name of the variable in df to replace its values.
        cond_fix (int or any): The condition to match in var_cond_fix to apply the replacement.
            Default is 2.
        value_var_fix_ori (int or any): The initial value to set to each generated variable.
            Default is 0.
        value_var_fix_fin (int or any): The final value to set to the variable var_fix that match the condition.
            Default is 1.

    Returns:
        pandas.DataFrame: The input DataFrame with the generated variables replaced according to the condition.
    """
    df[[col for col in list_fixed]] = value_var_fix_ori # generate
    df.loc[df[var_cond_fix]==cond_fix, var_fix]= value_var_fix_fin # replace
    return df
    
list_fixed_we = ["cuad2", "post", "n_neighborhood"]
var_cond_fix_we = 'distance_to_jewish_inst'
var_fix_we = 'cuad2'




        
def _rep_variables_we(df, type_of_condition, var_cond_rep, replace_var, condition_num=18, value_replace=1):
    
    """
    Generates a set of variables based on a fixed list, and replaces their values in a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame in which to generate and replace variables.
        list_fixed (list): A list of column names to generate.
        var_cond_fix (str): The name of the variable in df used to condition the replacement.
        var_fix (str): The name of the variable in df to replace its values.
        cond_fix (int or any): The condition to match in var_cond_fix to apply the replacement.
            Default is 2.
        value_var_fix_ori (int or any): The initial value to set to each generated variable.
            Default is 0.
        value_var_fix_fin (int or any): The final value to set to the variable var_fix that match the condition.
            Default is 1.

    Returns:
        pandas.DataFrame: The input DataFrame with the generated variables replaced according to the condition.

    """
    
    if type_of_condition == "bigger than":
        df.loc[df[var_cond_rep]>condition_num, replace_var]=value_replace
        return df
    elif type_of_condition == "smaller than":
        df.loc[df[var_cond_rep]<condition_num, replace_var]=value_replace
        return df
    elif type_of_condition == "equal to":
        df.loc[df[var_cond_rep]>condition_num, replace_var]=value_replace
        return df
    
type_of_condition_we = "bigger than"
var_cond_rep_we = 'week'
replace_var_we = 'post'




    
def _gen_diff_diff_variables_we(df, new_var_d, var1_d, var2_d, factor1_d=1, factor2_d=1):
    """
    Creates a new variable in a DataFrame based on the difference between two existing variables, optionally scaled by factors.

    Args:
        df (pandas.DataFrame): The DataFrame in which to create the new variable.
        new_var_d (str): The name of the new variable to create.
        var1_d (str): The name of the first existing variable to use in the difference.
        var2_d (str): The name of the second existing variable to use in the difference.
        factor1_d (float or int): A scaling factor for var1_d. Default is 1.
        factor2_d (float or int): A scaling factor for var2_d. Default is 1.

    Returns:
        pandas.DataFrame: The input DataFrame with the new variable added.

    """
    df[new_var_d] = (factor1_d*df[var1_d]) - (factor2_d*df[var2_d])
    return df

new_var_d_we = 'jewish_int_one_block_away_1'
var1_d_we = 'jewish_inst_one_block_away'
var2_d_we = 'jewish_inst'




def _gen_diff_sum_variables_we(df, new_var_s, var1_s, var2_s, factor1_s=1, factor2_s=1000):
    """
    Creates a new variable in a DataFrame based on the sum of two existing variables, optionally scaled by factors.

    Args:
        df (pandas.DataFrame): The DataFrame in which to create the new variable.
        new_var_s (str): The name of the new variable to create.
        var1_s (str): The name of the first existing variable to use in the sum.
        var2_s (str): The name of the second existing variable to use in the sum.
        factor1_s (float or int): A scaling factor for var1_s. Default is 1.
        factor2_s (float or int): A scaling factor for var2_s. Default is 1000.

    Returns:
        pandas.DataFrame: The input DataFrame with the new variable added.

    """
    df[new_var_s] = (factor1_s*df[var1_s]) + (factor2_s*df[var2_s])
    return df

new_var_s_we = 'code2'
var1_s_we = 'week'
var2_s_we = 'n_neighborhood'





def _gen_simple_we(df, new_var_sim, var_sim, factor_sim=(365/12)/7 ):
    """
    Creates a new variable in a DataFrame by multiplying an existing variable by a scaling factor.

    Args:
        df (pandas.DataFrame): The DataFrame in which to create the new variable.
        new_var_sim (str): The name of the new variable to create.
        var_sim (str): The name of the existing variable to use.
        factor_sim (float or int): A scaling factor to multiply var_sim with. 
            The default value corresponds to converting monthly values to weekly values.

    Returns:
        pandas.DataFrame: The input DataFrame with the new variable added.

    """
    df[new_var_sim] = df[var_sim]*factor_sim
    return df

new_var_sim_we = 'n_total_thefts'
var_sim_we = 'total_thefts'






def _gen_rep_variables_fixedlistcomplex_we(df, list1_fix_com, list2_fix_com, var_fix_comp_mul, list_rep_fix_com, var_fix_com_to_use='neighborhood', var_fix_com_to_change='n_neighborhood', range_rep_fix_com=range(1,4)): 
    """This function generates new variables based on existing variables in a dataframe and replaces the values of a specified variable based on certain conditions.
    
    Args:
    - df: pandas dataframe
    - list1_fix_com: list of column names in the dataframe to be multiplied with `var_fix_comp_mul` to generate new variables
    - list2_fix_com: list of column names in the dataframe to be used in the multiplication
    - var_fix_comp_mul: variable to be used as a multiplier in the multiplication
    - list_rep_fix_com: list of values of `var_fix_com_to_use` to be replaced in `var_fix_com_to_change`
    - var_fix_com_to_use: name of the column in the dataframe to use as condition for replacement (default: 'neighborhood')
    - var_fix_com_to_change: name of the column in the dataframe to be replaced (default: 'n_neighborhood')
    - range_rep_fix_com: range of values to use for replacement (default: range(1, 4))
    
    Returns:
    - df: pandas dataframe with new variables generated and specified variable replaced based on conditions
    """
    for col1, col2 in zip(list1_fix_com, list2_fix_com):
        df[col1] = df[col2]*df[var_fix_comp_mul]
    for col, i in zip(list_rep_fix_com, range_rep_fix_com):
        df.loc[df[var_fix_com_to_use]==col, var_fix_com_to_change]=i
    return df
    
list1_fix_com_we = ["jewish_inst_p", "jewish_int_one_block_away_1_p", "cuad2p"]
list2_fix_com_we = ["jewish_inst",  "jewish_int_one_block_away_1", "cuad2"]
var_fix_comp_mul_we = 'post'
list_rep_fix_com_we = ["Belgrano", "Once", "V. Crespo"]



def _df_to_csv(df, location):
    """
    Saves a pandas DataFrame to a CSV file at the specified location.

    Args:
        df (pandas DataFrame): The DataFrame to be saved.
        location (str): The file path and name for the CSV file, including the ".csv" extension.

    Returns:
        None
    """
    df.to_csv(location)  

location_we = '/Users/bonjour/Documents/Master in Economics Bonn/3rd semester/Programming practices/Final work/Possible papers/Do Police reduce crime/Github/di_tella_2004_replication/src/di_tella_2004_replication/data_management/Clean_Data/WeeklyPanel.csv'




"""Everything inside a function"""

def weeklypanel(df, 
                list_drop,
                var_cond_ext,
                list_fixed, var_cond_fix, var_fix,
                type_of_condition, var_cond_rep, replace_var,
                new_var_d, var1_d, var2_d,
                new_var_s, var1_s, var2_s,
                new_var_sim, var_sim,
                list1_fix_com, list2_fix_com, var_fix_comp_mul, list_rep_fix_com,
                location):
    df = _clean_column_names_we(df)
    df = _drop_variables_we(df, list_drop)
    df = _gen_rep_variables_fixedextension_we(df, var_cond_ext)
    df = _gen_rep_variables_fixedlistsimple_we(df, list_fixed, var_cond_fix, var_fix)
    df = _rep_variables_we(df, type_of_condition, var_cond_rep, replace_var)
    df = _gen_diff_diff_variables_we(df, new_var_d, var1_d, var2_d)
    df = _gen_diff_sum_variables_we(df, new_var_s, var1_s, var2_s)
    df = _gen_simple_we(df, new_var_sim, var_sim)
    df = _gen_rep_variables_fixedlistcomplex_we(df, list1_fix_com, list2_fix_com, var_fix_comp_mul, list_rep_fix_com)
    _df_to_csv(df, location)
    
WeeklyPanel = weeklypanel(df=WeeklyPanel,
                list_drop=list_drop_we,
                var_cond_ext=var_cond_ext_we,
                list_fixed=list_fixed_we, var_cond_fix=var_cond_fix_we, var_fix=var_fix_we,           
                type_of_condition=type_of_condition_we, var_cond_rep=var_cond_rep_we, replace_var=replace_var_we,
                new_var_d=new_var_d_we, var1_d=var1_d_we, var2_d=var2_d_we,
                new_var_s=new_var_s_we, var1_s=var1_s_we, var2_s=var2_s_we,
                new_var_sim=new_var_sim_we, var_sim=var_sim_we,
                list1_fix_com=list1_fix_com_we, list2_fix_com=list2_fix_com_we, var_fix_comp_mul=var_fix_comp_mul_we, list_rep_fix_com=list_rep_fix_com_we,
                location=location_we)
