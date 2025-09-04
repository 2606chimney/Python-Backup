## Azura_BiWeekly_02_26_2024.py


## Azura-Edo Dashboard, Bi-Weekly
## Azura-Edo, Bi Weekly Condition Dashboard- 11/14/2023
# 1. Universal version for GT-11, GT-12, and GT-13
# 2. Moving data files to a different directory
# 3. Power Factor Calculation- 11/27/2023
# 4. Header Modify- 11/28/2023
# 5. Turbine Comparison- 11/30/2023
# 6. Modified for Average Calc- 02/26/2024



#### Import Python Packages
import pandas as pd
import numpy as np
import math
import csv
import re

import operator
import sys
import pyodbc
import pytz
import shutil, os
import datetime as dt

from numpy import mean
from pandas import DataFrame
from datetime import date, datetime, timedelta



#### Grabbing All Data Files from the Directory
Directory = 'C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Azura/Bi-Weekly Data'
file_list = os.listdir(Directory)


#### Reading Data Files and Process
for i in range(3):
    unit_num = file_list[i][0:4]
    
    ## Grabbing All the Data File Name in the Directory
    full_path = str(Directory) + '/' + file_list[i]
    dfx = pd.read_csv(full_path, encoding='unicode_escape')
    
    ## Replacing NaN with Zeros
    df = dfx.fillna(0)
    del dfx
    
    ## Moving the Data File to Another Directory
    new_path = 'C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Azura/Documents/Old_Data_Files'
    new_direct = str(new_path) + '/' + file_list[i]
    shutil.move(full_path, new_direct)
    
    ## Defining Filenames
    short_tag_desc_filename = 'Azura_TagDesc_List_allGTs.xlsx' # Tag Description File Name
    short_tag_sheet = unit_num + '_List'  # Sheet Name
    final_result_file = unit_num + '_biweekly_conditions.csv' # Creating Final Result file Name
    
    ## Remove headers and setup a column names
    # df.columns = [''] * len(df.columns) # Deleting 'Unnamed' header
    # df.columns = df.iloc[0] # Assign the first row as a column name
    # df = df.drop(labels=[0,1,2], axis=0) # Remove the first to third row from the dataframe
    # df1 = df.reset_index() # Reset index number
    # df = df1.drop('index', axis=1) # Dropping 'index' column
    # del df1
    
    ## Split DataTime column from the table
    df1 = df.iloc[:, 0] # Split the first column (Date) from the rest
    df1_datetime = pd.DataFrame(df1)
    df1_datetime.columns = ['datetime'] # Assign a new column name, 'DateTime'

    ## Split rest of columns from the table
    df2 = df.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

    ## Combine two sub-tables into one
    df1_datetime = df1_datetime.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)
    df_corrted = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
    del df1_datetime, df1, df2, df
    
    df = df_corrted.copy()
    del df_corrted
    
    ## Sanitize DateTime column
    DT = df['datetime']

    # Trimming 'datetime' strings
    DateTime = []
    for i in range(len(DT)):
        string = DT[i]
        short_str = string[0:19]
        DateTime.append(short_str)
    df['DateTime'] = DateTime

    # Move 'DateTime' column into front column
    Date = df['DateTime']
    df.drop(labels=['datetime'], axis=1, inplace=True)
    df.drop(labels=['DateTime'], axis=1, inplace=True)
    df.insert(0,'DateTime', Date)
    del DT, DateTime, Date
    
    # Read Tag Description List and Replace with Short Desc
    df_tag_desc = pd.read_excel(short_tag_desc_filename, sheet_name = short_tag_sheet)
    desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only
    
    ## Read column names from the table
    col_list = df.columns.values.tolist()

    # Rename 'column codes' to 'column descriptions'
    for i in range(len(desc_list)):
        df.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
    del df_tag_desc, desc_list, col_list
    
    ## Active Power in % Calculation
    maxpower = 168 # mw
    df['DWATT, %'] = (df['ACTIVE POWER'] / maxpower) * 100  # DWATT calculation in %
    df['DWATT >= 85%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 85 else 0)
    df['DWATT >= 70%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 70 else 0)
    
    ## Calendar
    df1 = df.copy()
    df1['DateTime'] = pd.to_datetime(df['DateTime'])
    df1['Date'] = df1['DateTime'].dt.date
    df1['Year'] = df1['DateTime'].dt.year
    df1['Month'] = df1['DateTime'].dt.month
    df1['Day'] = df1['DateTime'].dt.day
    df1['Hour'] = df1['DateTime'].dt.hour
    df = df1.copy()
    del df1
    
    ## Power Factor Calculation-- Added on 11/27/2023
    P = df['ACTIVE POWER'] ** 2
    Q = df['REACTIVE POWER'] ** 2
    SQRT = (P + Q) ** (1/2)
    df['Calc_PF'] = df['ACTIVE POWER'] / SQRT
    df['Calc_PF'] = df['Calc_PF'].astype(float)
    
    ## Saving the Results
    df.to_csv(final_result_file, index=False, float_format='%.2f')
    df = pd.read_csv(final_result_file)
    final_direct = str('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Azura/') + final_result_file
    df.to_csv(final_direct, index=False)



#### Reading Data Files
Directory = 'C:/Users/Chongchan.Lee/SQL_Codes_Deployed/AzuraEdo/Bi_weekly_Condition'
file_list = ['GT11_biweekly_conditions.csv', 'GT12_biweekly_conditions.csv', 'GT13_biweekly_conditions.csv']

##--------------------------- GT Comparisons -----------------------------##
#### Reading Data Files
for i in range(3):   
    File = file_list[i]
    full_path = str(Directory) + '/' + File
    
    if i == 0:
        df_biweek = pd.read_csv(full_path)
        ## Creating a empty dataframe
        df_compare = pd.DataFrame()
        
    elif i == 1:
        df_biweek = pd.read_csv(full_path)
        
    else:
        df_biweek = pd.read_csv(full_path)
        
    ## Creating a New Dataframe
    useful_col = ['DateTime',
                  'Date',
                  'Year',
                  'Month',
                  'Day',
                  'Hour',
                  'FLUEGAS CO VAL SCALED',
                  'FLUEGAS NOx VAL SCALED',
                  'REL S-VIB TURB BRG',
                  'REL S-VIB COMPR BRG',
                  'DP PULSE FILTER',
                  'DP FILTER',
                  'COALESCE FILT DP',
                  'DIFF PRES C-C AVE',
                  'TEMP DIFFUSOR OUTLET_Avg',
                  'PRES BRG OIL FEED',
                  'TEMP BRG OIL FEED',
                  'ROTOR LIFT OIL SYSTEM',
                  'PRES HYD SYSTEM FEED',
                  'ROTOR TEMP CALC MHS_3',
                  'ROTOR TEMP CALC CVC2_4',
                  'ROTOR TEMP CALC CCAS_5',
                  'ROTOR TEMP CALC TD4_6',
                  'ACTIVE POWER',
                  'CALC TURB OT',
                  'TEMP CA D/STR CLR TE',
                  'TEMP CA D/STR CLR EE',
                  'TEMP HOT AIR AHD CLR',
                  'GEN VOLTAGE',
                  'GEN ROTOR TEMPERATURE',
                  'REL S-VIB GEN BRG TE',
                  'REL S-VIB GEN BRG EE',
                  'DWATT, %',
                  'DWATT >= 85%',
                  'DWATT >= 70%',
                  'Calc_PF']
    df_new = df_biweek[useful_col]

    ## Mean value calculations
    df_avg = df_new.copy()
    df_avg['SHAFT POSN MEAS_AVG'] = df_biweek.loc[:, ['SHAFT POSN MEAS_1','SHAFT POSN MEAS_2']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['TURBINE SPEED_AVG'] = df_biweek.loc[:, ['TURBINE SPEED_1','TURBINE SPEED_2','TURBINE SPEED_3']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['TEMP COMPR INLET_AVG'] = df_biweek.loc[:, ['TEMP COMPR INLET_1B','TEMP COMPR INLET_2B','TEMP COMPR INLET_3B']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['PRES COMPR OUTLET_AVG'] = df_biweek.loc[:, ['PRES COMPR OUTLET_1','PRES COMPR OUTLET_2']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['TEMP COMPR OUTLET_AVG'] = df_biweek.loc[:, ['TEMP COMPR OUTLET_1A','TEMP COMPR OUTLET_1B','TEMP COMPR OUTLET_2A','TEMP COMPR OUTLET_2B']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['TEMP TURB OUTLET_AVG'] = df_biweek.loc[:, ['TEMP TURB OUTLET_2B','TEMP TURB OUTLET_3B','TEMP TURB OUTLET_4B','TEMP TURB OUTLET_6B','TEMP TURB OUTLET_7B','TEMP TURB OUTLET_8B']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['TEMP TURB BRG_AVG'] = df_biweek.loc[:, ['TEMP TURB BEARING_1A','TEMP TURB BEARING_1B','TEMP TURB BEARING_1C']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['TEMP THRUST BRG GE_AVG'] = df_biweek.loc[:, ['TEMP THRUST BRG GE_02A','TEMP THRUST BRG GE_03A','TEMP THRUST BRG GE_03B','TEMP THRUST BRG GE_12A','TEMP THRUST BRG GE_13A']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['TEMP THRUST BRG TE_AVG'] = df_biweek.loc[:, ['TEMP THRUST BRG TE_04A','TEMP THRUST BRG TE_05A','TEMP THRUST BRG TE_05B','TEMP THRUST BRG TE_14A','TEMP THRUST BRG TE_15A']].mean(axis=1)
    df_new = df_avg.copy()
    
    df_avg = df_new.copy()
    df_avg['TEMP COMPR BRG_AVG'] = df_biweek.loc[:, ['TEMP COMPR BEARING_01A','TEMP COMPR BEARING_01B','TEMP COMPR BEARING_01C']].mean(axis=1)
    df_new = df_avg.copy()
    
    df_avg = df_new.copy()
    df_avg['TEMP GEN BRG TE_AVG'] = df_biweek.loc[:, ['TEMP GEN BEARING TE_A','TEMP GEN BEARING TE_B', 'TEMP GEN BEARING TE_C']].mean(axis=1)
    df_new = df_avg.copy()
    
    df_avg = df_new.copy()
    df_avg['TEMP GEN BRG EE_AVG'] = df_biweek.loc[:, ['TEMP GEN BEARING EE_A','TEMP GEN BEARING EE_B', 'TEMP GEN BEARING EE_C']].mean(axis=1)
    df_new = df_avg.copy()
    
    df_avg = df_new.copy()
    df_avg['TEMP LUBE OIL TANK_AVG'] = df_biweek.loc[:, ['TEMP LUBE OIL TANK_1A','TEMP LUBE OIL TANK_1B']].mean(axis=1)
    df_new = df_avg.copy()
    
    df_avg = df_new.copy()
    df_avg['TEMP STATOR SLOT_AVG'] = df_biweek.loc[:, ['TEMP STR SLOT_01','TEMP STR SLOT_02','TEMP STR SLOT_03','TEMP STR SLOT_04','TEMP STR SLOT_05','TEMP STR SLOT_06','TEMP STR SLOT_07','TEMP STR SLOT_08','TEMP STR SLOT_09','TEMP STR SLOT_10','TEMP STR SLOT_11','TEMP STR SLOT_12']].mean(axis=1)
    df_new = df_avg.copy()
    
    df_avg = df_new.copy()
    df_avg['VIB TURB BRG CSG_AVG'] = df_biweek.loc[:, ['VIB TURB BEARING CSG_1','VIB TURB BEARING CSG_2']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['VIB COMPR BRG CSG_AVG'] = df_biweek.loc[:, ['VIB COMPR BRG CSG_1','VIB COMPR BRG CSG_2']].mean(axis=1)
    df_new = df_avg.copy()
    
    df_avg = df_new.copy()
    df_avg['VIB GEN BRG CSG TE_AVG'] = df_biweek.loc[:, ['VIB GEN BRG CSG TE_21','VIB GEN BRG CSG TE_22']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['VIB GEN BRG CSG EE_AVG'] = df_biweek.loc[:, ['VIB GEN BRG CSG EE_21','VIB GEN BRG CSG EE_22']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['C-C REL PRES DISSIP_AVG'] = df_biweek.loc[:, ['C-C REL PRES DISSIP_XQ1','C-C REL PRES DISSIP_XQ2']].mean(axis=1)
    df_new = df_avg.copy()

    df_avg = df_new.copy()
    df_avg['HUM MONITORING C_AVG'] = df_biweek.loc[:, ['HUM MONITORING C-CL','HUM MONITORING C-CR']].mean(axis=1)
    df_new = df_avg.copy()

    ## Column Name Change
    col_name_list = df_new.columns.values.tolist()
    column_names = col_name_list[6:]

    for j in range(len(column_names)): 
        old_col_name = column_names[j]
        if i == 0:
            new_col_name = old_col_name + '_gt11'
        elif i == 1:
            new_col_name = old_col_name + '_gt12'
        else:
            new_col_name = old_col_name + '_gt13'      
        df_new.rename(columns={old_col_name:new_col_name}, inplace=True)

    new_col_name_list = df_new.columns.values.tolist()

    if i == 0:
        df_compare = df_new.copy()
    elif i ==1:
        df_new.drop(df_new.columns[[1, 2, 3, 4, 5]], axis=1, inplace=True)
        df_compare = df_compare.merge(df_new, on = 'DateTime', how = 'left')
    else:
        df_new.drop(df_new.columns[[1, 2, 3, 4, 5]], axis=1, inplace=True)
        df_compare = df_compare.merge(df_new, on = 'DateTime', how = 'left')

        
#### Saving a Final Result
df_compare.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Azura/gt_comparison.csv', index=False, float_format='%.2f')