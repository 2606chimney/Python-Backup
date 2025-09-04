#!/usr/bin/env python

# # Quisqueya Condition Monitoring- Temp & Vib
# Quisqueya Daily Query- 1-hour Sampling- 5/25/2023
# Managing All Zero Or Missing Values on A Single Day-- 6/9/2023
# For Previous Day Query
# 1. New Tags (Exhaust Gas Temp, Inlet TC A/B and Outlet TC A/B) are added,
# 2. ST tags are removed- 6/20/2023
# 3. Peak torsional vibration tags are added- 10/19/2023
# 4. Line for duplicate data removal due to Daylight Savings Time is added at the last stage= 11/6/2023

# Published on 10/20/2023


#### Importing Python Packages
import pandas as pd
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



#### Single-Day Configuration
today = datetime.today()
Today = pd.to_datetime('today').normalize()
Yesterday = Today - timedelta(days = 1)
start_t = Yesterday
end_t = Today

# Changing UTC to EST time
time_change_start = timedelta(hours=1)
time_change_end = timedelta(hours=6)

# Start Time
start_datetime = start_t - time_change_start

# End Time
end_datetime = end_t + time_change_end


#### Read Tag List
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='QQY_Monitoring_daily_v2')
dim = len(df_list)


#### SQL Configuration
cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Abbrev_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor() 


#### SQL Querry: 1-Hour average
for i in df_tag[:dim].index:
    sql = '''SELECT Timestamp, "{}:Value:Average" as rowValue FROM History_1h WHERE Timestamp BETWEEN "{}" AND "{}"'''.format(df_tag['Tag_List'][i], start_datetime, end_datetime)
    cursor.execute(sql) 
    rows = cursor.fetchall()

    DateTime = []
    Values = []
    for row in rows:   
        if i == 0:
            DateTime.append(row.Timestamp)
            Values.append(row.rowValue)
        else:
            Values.append(row.rowValue)

    if i == 0:
        df_date = pd.DataFrame(DateTime, columns=['TimeStamp'])
        tag_name = df_list['Abbrev_Name'].iloc[i]
        df_val = pd.DataFrame(Values, columns=[tag_name])
        dfx = pd.concat([df_date, df_val], axis=1)
        frames = dfx.copy()
    else:
        tag_name = df_list['Abbrev_Name'].iloc[i]
        df_val = pd.DataFrame(Values, columns=[tag_name])
        dfx = pd.concat([frames, df_val], axis=1)
        frames = dfx.copy()
del frames

# Converting UTC time scale to US EST time scale
dfx['TimeStamp'] = pd.to_datetime(dfx['TimeStamp'], errors='coerce')
dfx['TimeStamp_EST'] = dfx['TimeStamp'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S')
EST_Time = dfx['TimeStamp_EST']
dfx.drop(labels=['TimeStamp'], axis=1, inplace=True)
dfx.insert(0,'TimeStamp', EST_Time)
dfx.drop(labels=['TimeStamp_EST'], axis=1, inplace=True)


#### Date Correction
# For 1-day
Date_1 = datetime.strptime(str(start_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
Date_2 = datetime.strptime(str(end_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# Remove Exceeding Date
df1 = dfx[dfx['TimeStamp'] >= str(Date_1)]
df2 = df1[df1['TimeStamp'] <= str(Date_2)]
df2.reset_index(drop=True, inplace=True)
df = df2.copy()
del df1, df2, df_list, dfx


#### For Steam Turbine
## ST Running
# df['ST_Running'] = df['ST_RPM'].apply(lambda x: 1 if x > 0.95*x  else 0) # Turned off


#### Calendar
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
df['Date'] = df['TimeStamp'].dt.date
df['Month'] = df['TimeStamp'].dt.month
df['Day'] = df['TimeStamp'].dt.day
df['Hour'] = df['TimeStamp'].dt.hour


#### Saving Single-Day Data into a CSV File
df_1day = df.copy()
df.to_csv('QQY_10day_Query_v3.csv', mode='a', index=False, header=False)
del df


#### Retain 10-Day Data
## Read Saved Data File
df = pd.read_csv('QQY_10day_Query_v3.csv')

## Remove Duplicated Rows
dfx = df.drop_duplicates(subset=['TimeStamp'], keep='first')
del df

## Remove Date Older Than 10 Days
day_num = dfx['Day'].unique()
num_day = len(day_num)
#-- Added on 8/22/2024 --
if num_day > 10:
    day_gone = dfx['Day'].iloc[0]
    df = dfx[dfx['Day'] != day_gone]
#-- End of Addition --
df.reset_index(drop=True, inplace=True)
del dfx

## Saving a Final 10-Day Rolling Query Data
df.to_csv('QQY_10Day_Query_v3.csv', index=False, float_format='%.2f')
del df


####------------------ Outlier Detection -----------------

#### Replacing NaN with Zeros
df_CRT = df_1day.fillna(0)
del df_1day


#### Corrected Engine Running Status Tags
engine_no = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

## Engine No.
for i in range(len(engine_no)):
    name = 'Engine' + engine_no[i] + '_Running'
    j = df_CRT.columns.get_loc(name) # Finding Column Location
    new_col_name = name + '_CRT'
    
    # Corrected Engine Running Status
    df_CRT[new_col_name]  = df_CRT[name].apply(lambda x: x if x == 1 else 0)
    
    # Column Re-positioning
    col_to_move = df_CRT.pop(new_col_name)
    df_CRT.insert(j+1, new_col_name, col_to_move)
    df_CRT.drop(name, axis=1, inplace=True)


#### Corrected Temp Values
tag_list = ['_Gen_Wind_L1_temp',
            '_Gen_Wind_L2_temp',
            '_Gen_Wind_L3_temp',
            '_Gen_Bear_Drive_temp',
            '_Gen_Bear_Nondrive_temp',
            '_Torsion_vib',
            '_Torsion_vib_mean',
            '_Torsion_vib_pk',
            '_Main_Bear00_temp',
            '_Main_Bear01_temp',
            '_Main_Bear02_temp',
            '_Main_Bear03_temp',
            '_Main_Bear04_temp',
            '_Main_Bear05_temp',
            '_Main_Bear06_temp',
            '_Main_Bear07_temp',
            '_Main_Bear08_temp',
            '_Main_Bear09_temp',
            '_Main_Bear10_temp',
            '_Bigend_Bear_Cyl01A_temp',
            '_Bigend_Bear_Cyl01B_temp',
            '_Bigend_Bear_Cyl02A_temp',
            '_Bigend_Bear_Cyl02B_temp',
            '_Bigend_Bear_Cyl03A_temp',
            '_Bigend_Bear_Cyl03B_temp',
            '_Bigend_Bear_Cyl04A_temp',
            '_Bigend_Bear_Cyl04B_temp',
            '_Bigend_Bear_Cyl05A_temp',
            '_Bigend_Bear_Cyl05B_temp',
            '_Bigend_Bear_Cyl06A_temp',
            '_Bigend_Bear_Cyl06B_temp',
            '_Bigend_Bear_Cyl07A_temp',
            '_Bigend_Bear_Cyl07B_temp',
            '_Bigend_Bear_Cyl08A_temp',
            '_Bigend_Bear_Cyl08B_temp',
            '_Bigend_Bear_Cyl09A_temp',
            '_Bigend_Bear_Cyl09B_temp',
            '_Exh_Inlet_TC_A_temp',
            '_Exh_Outlet_TC_A_temp',
            '_Exh_Inlet_TC_B_temp',
            '_Exh_Outlet_TC_B_temp']

engine_no = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

## Engine No.
for i in range(len(engine_no)):
    engn_no = 'Engine' + engine_no[i]
    engn_run_name = engn_no + '_Running_CRT'

    for j in range(len(tag_list)):
        col_name = engn_no + tag_list[j]
        m = df_CRT.columns.get_loc(col_name) # Finding Column Location
        new_col_name = col_name + '_CRT'
        
        # Corrected Temp Values
        df_TEMPO = df_CRT.copy()
        df_TEMPO[new_col_name]  = df_TEMPO[engn_run_name] * df_TEMPO[col_name]
        
        # Column Re-positioning
        col_to_move = df_TEMPO.pop(new_col_name)
        df_TEMPO.insert(m+1, new_col_name, col_to_move)
        df_TEMPO.drop(df_TEMPO.columns[m], axis=1, inplace=True) 
        df_CRT = df_TEMPO.copy()
del df_TEMPO


#### Saving Ten-Day Data into a CSV File
df_CRT.to_csv('QQY_10Day_CORRECT_v3.csv', mode='a', index=False, header=False)


#### Read Saved Data File
dfx = pd.read_csv('QQY_10Day_CORRECT_v3.csv')


#### Remove Date Older Than 10 Days
day_num = dfx['Day'].unique()
num_day = len(day_num)
day_gone = dfx['Day'].iloc[0]
df = dfx[dfx['Day'] != day_gone]
df.reset_index(drop=True, inplace=True)
del dfx


#### Saving a Corrected Data
df.to_csv('QQY_10Day_CORRECT_v3.csv', index=False)
del df


#### Calculations for Fleet Upper, Lower Limits and Mean
tag_list = ['_Gen_Wind_L1_temp_CRT',
            '_Gen_Wind_L2_temp_CRT',
            '_Gen_Wind_L3_temp_CRT',
            '_Gen_Bear_Drive_temp_CRT',
            '_Gen_Bear_Nondrive_temp_CRT',
            '_Torsion_vib_CRT',
            '_Torsion_vib_mean_CRT',
            '_Torsion_vib_pk_CRT',
            '_Main_Bear00_temp_CRT',
            '_Main_Bear01_temp_CRT',
            '_Main_Bear02_temp_CRT',
            '_Main_Bear03_temp_CRT',
            '_Main_Bear04_temp_CRT',
            '_Main_Bear05_temp_CRT',
            '_Main_Bear06_temp_CRT',
            '_Main_Bear07_temp_CRT',
            '_Main_Bear08_temp_CRT',
            '_Main_Bear09_temp_CRT',
            '_Main_Bear10_temp_CRT',
            '_Bigend_Bear_Cyl01A_temp_CRT',
            '_Bigend_Bear_Cyl01B_temp_CRT',
            '_Bigend_Bear_Cyl02A_temp_CRT',
            '_Bigend_Bear_Cyl02B_temp_CRT',
            '_Bigend_Bear_Cyl03A_temp_CRT',
            '_Bigend_Bear_Cyl03B_temp_CRT',
            '_Bigend_Bear_Cyl04A_temp_CRT',
            '_Bigend_Bear_Cyl04B_temp_CRT',
            '_Bigend_Bear_Cyl05A_temp_CRT',
            '_Bigend_Bear_Cyl05B_temp_CRT',
            '_Bigend_Bear_Cyl06A_temp_CRT',
            '_Bigend_Bear_Cyl06B_temp_CRT',
            '_Bigend_Bear_Cyl07A_temp_CRT',
            '_Bigend_Bear_Cyl07B_temp_CRT',
            '_Bigend_Bear_Cyl08A_temp_CRT',
            '_Bigend_Bear_Cyl08B_temp_CRT',
            '_Bigend_Bear_Cyl09A_temp_CRT',
            '_Bigend_Bear_Cyl09B_temp_CRT',
            '_Exh_Inlet_TC_A_temp_CRT',
            '_Exh_Outlet_TC_A_temp_CRT',
            '_Exh_Inlet_TC_B_temp_CRT',
            '_Exh_Outlet_TC_B_temp_CRT']

engine_no = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

## Engine No.
for i in range(len(tag_list)):
    ## Engine No.
    tag_name = tag_list[i]
    
    ## TimeStamp-wise (24-data points for 24-hour using Engin01_Running)
    Y = []
    Upper_fleet = []
    Lower_fleet = []
    Mean_fleet = []
    for j in range(len(df_CRT.iloc[:,1])):

        ## EngineNumer-wise (for 12-Engine)
        Y = []
        for k in range(len(engine_no)):
            full_tag_name = 'Engine' + engine_no[k] + tag_name
            tag_val = df_CRT[full_tag_name].iloc[j]
            Y.append(tag_val)
            
        # Tag Name in Short
        short_name = full_tag_name[9:-4]
        
        # Removing Zero Values
        Z = [x for x in Y if x != 0]    
        del tag_val, Y

        ## Detecting Null List
        if not Z: # if Z is empty
            # Upper & lower bound
            Upper = 0
            Lower = 0
            
            # Storing the Calculated Values in a Series
            Upper_fleet.append(Upper)
            Lower_fleet.append(Lower)
            
        else: # if Z is not empty
            # IQR
            Q1 = np.percentile(Z, 25, interpolation = 'midpoint') # Back on this on 8-22-2024
            Q3 = np.percentile(Z, 75, interpolation = 'midpoint') # Back on this on 8-22-2024
            #Q1 = np.percentile(Z, 10, interpolation = 'midpoint') # Modified on 1/4/2024
            #Q3 = np.percentile(Z, 90, interpolation = 'midpoint') # Modified on 1/4/2024
            IQR = Q3 - Q1
    
            # Upper & lower bound
            Upper = Q3 + 1.5 * IQR
            Lower = Q1 - 1.5 * IQR
        
            # Removing the Outliers
            Z_upper = [i for i in Z if i <= Upper]
            Z_lower = [i for i in Z if i >= Lower]
            
        # Storing the Calculated Values in a Series
        Upper_fleet.append(Upper)
        Lower_fleet.append(Lower)
#         Mean_fleet.append(Mean_temp) #---Turned Off

    Upper_fleet = pd.Series(Upper_fleet)
    Lower_fleet = pd.Series(Lower_fleet)
#     Mean_fleet = pd.Series(Mean_fleet) #--- Turned Off

    short_name_upper = short_name + '_Upper'
    short_name_lower = short_name + '_Lower'
#     short_name_mean = short_name + '_Mean' #--- Turned Off

    df_calc = df_CRT.copy()
    df_calc[short_name_upper] = Upper_fleet
    df_calc[short_name_lower] = Lower_fleet
#     df_calc[short_name_mean] = Mean_fleet #--- Turned Off
    df_CRT = df_calc.copy()
del df_calc      


#### Concatenating Resulting Data
df_CRT.to_csv('QQY_Outlier_Results_v3.csv', mode='a', index=False, header=False)
del df_CRT


#### Reading the Saved Data File
dfx = pd.read_csv('QQY_Outlier_Results_v3.csv')

## Remove Date Older Than 10 Days
day_num = dfx['Day'].unique()
num_day = len(day_num)
day_gone = dfx['Day'].iloc[0]
df = dfx[dfx['Day'] != day_gone]
df.reset_index(drop=True, inplace=True)
del dfx

## Remove Duplicated Rows- Added on 11/6/2023
df1 = df.drop_duplicates(subset=['TimeStamp'], keep='first')
del df

#### Saving a Final 10-Day Rolling Data
df1.to_csv('QQY_Outlier_Results_v3.csv', index=False, float_format='%.2f')
df1.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/QQY_Outlier_Results_v3.csv', index=False, float_format='%.2f')

