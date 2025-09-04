#!/usr/bin/env python

## SHOEI Daily Condition Monitoring
## 01/30/2023

## Daily Condition Monitoring, 10-day rolling window


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


# #### 10-Day Configuration
# today = datetime.today()
# today = pd.to_datetime('today').normalize()
# ## 10-day Span
# startDate = today - timedelta(days = 11)
# endDate = today - timedelta(days = 1)  
# ## Changing UTC to EST time
# time_change_start = timedelta(hours=1)
# time_change_end = timedelta(hours=6)
# ## Start Time
# start_datetime = startDate - time_change_start
# ## End Time
# end_datetime = endDate + time_change_end
# print('Starting Date: ', start_datetime)
# print('Ending Date: ', end_datetime)

## Single-Day Configuration
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


#### Part-I, Data Query
## Read Tag List
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='SHO_Monitoring_daily')
dim = len(df_list)

## SQL Configuration
cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Short_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor()

## SQL Querry
for i in df_tag[:dim].index:
    sql = '''SELECT Timestamp, "{}:Value:Average" as rowValue FROM History_10m WHERE Timestamp BETWEEN "{}" AND "{}"'''.format(df_tag['Tag_List'][i], start_datetime, end_datetime)
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

## Date Correction
# # For 10-day
# Date_1 = datetime.strptime(str(startDate), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
# Date_2 = datetime.strptime(str(endDate), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# For 1-day
Date_1 = datetime.strptime(str(start_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
Date_2 = datetime.strptime(str(end_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# Remove Exceeding Date
df1 = dfx[dfx['TimeStamp'] >= str(Date_1)]
df2 = df1[df1['TimeStamp'] <= str(Date_2)]
df = df2.copy()
del df1, df2, df_list, dfx

# Remove Duplicated Rows
df = df.drop_duplicates(keep='first')


#### Preprocessing
# Calendar
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
df['date'] = df['TimeStamp'].dt.date
df['month'] = df['TimeStamp'].dt.month
df['day'] = df['TimeStamp'].dt.day

# Move 'DateTime' column into front column
Date = df['day']
df.drop(labels=['day'], axis=1, inplace=True)
df.insert(1,'Day', Date)

Date = df['month']
df.drop(labels=['month'], axis=1, inplace=True)
df.insert(1,'Month', Date)

Date = df['date']
df.drop(labels=['date'], axis=1, inplace=True)
df.insert(1,'Date', Date)

# Listing 'object' columns 
obj_cols = []
for i in df.columns:
    if df[i].dtype == 'object':
        obj_cols.append(i)

# Converting 'object' data type to 'float' data type
for i in range(len(obj_cols)):
    if i != 0:
        df[obj_cols[i]] = pd.to_numeric(df[obj_cols[i]], errors='coerce')


#### Appending 1-day data to an Existing File
df.to_csv('SHO_10day_Query.csv', mode='a', index=False, header=False)
##### Writing 10-Day Data into a CSV File
# df.to_csv('SHO_10day_Data.csv', index=False)


#### Trim the Date Length, 10-Day
## Read Data
dfx = pd.read_csv('SHO_10day_Query.csv')

## Remove Duplicated Rows
df = dfx.drop_duplicates(subset=['TimeStamp'], keep=False)
del dfx


#### Remove Date Older Than 10 Days/ Write Data to a CSV file
day_num = df['Day'].unique()
num_day = len(day_num)
day_gone = df['Day'].iloc[0]
df1 = df[df['Day'] != day_gone]

## Saving Query Data
df1.to_csv('SHO_10day_Query.csv', index=False)
del df, df1



#-------------------- PART-2: Preprocessing Data --------------------------
#### Read Data
df = pd.read_csv('SHO_10day_Query.csv')

## Replacing NaN with Zeros
df = df.fillna(0)


#### Production Forecasting
## Capacity: IVT-1 = 100 KW, IVT-2 = 40 KW, IVT-3 = 40 KW

# IVT-1_KWAC Forecast Using a 100KW Fit Curve
ivt_1_estimated = ((0.1126 * df['MET-1_HalfCellRad1']) + 0.5091)

# IVT-2_KWAC Forecast Using a 80KW Fit Curve
ivt_2_estimated = ((0.046 * df['MET-1_HalfCellRad1']) + 0.3958)

# IVT-3_KWAC Forecast Using a 80KW Fit Curve
ivt_3_estimated = ((0.045 * df['MET-1_HalfCellRad1']) + 0.4756)

# Saving Forecast into the DataFrame in kW
dfx = df.copy()
dfx['estimated_IVT_1_KWAC'] = ivt_1_estimated
dfx['estimated_IVT_2_KWAC'] = ivt_2_estimated
dfx['estimated_IVT_3_KWAC'] = ivt_3_estimated
del ivt_1_estimated, ivt_2_estimated, ivt_3_estimated

# Threshold Calculation
dfx['estimated_IVT_1_KWAC_thresh'] = 0.85 * dfx['estimated_IVT_1_KWAC']
dfx['estimated_IVT_2_KWAC_thresh'] = 0.85 * dfx['estimated_IVT_2_KWAC']
dfx['estimated_IVT_3_KWAC_thresh'] = 0.85 * dfx['estimated_IVT_3_KWAC']


#### String Level DC Power
ivt = ['IVT-1', 'IVT-2', 'IVT-3']
amps_1 = ['01','02','03','04','05','07','09','11','13','15','17','18','19','20']
volts_1 = ['01','02','03','04','05','07','09','11','13','15','17','18','19','20']
amps_2 = ['01','03','04','05','07','08']
volts_2 = ['01','03','04','05','07','08']
string_1 = 'StringAmps'
string_2 = 'StringVolt'

dfpower = pd.DataFrame()
for i in range(0,3):
    ivt_str = ivt[i] + '_'
    
    if i == 0:
        for j in range(0, len(amps_1)):
            amp_str = ivt_str + string_1 + amps_1[j]
            volt_str = ivt_str + string_2 + volts_1[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name =  ivt_str + string_1[0:6] + amps_1[j] + '_dcpower'
            dfx[col_name] = dcpower

    else:
        for k in range(0, len(amps_2)):
            amp_str = ivt_str + string_1 + amps_2[k]
            volt_str = ivt_str + string_2 + volts_2[k]
            dcpower = df[amp_str] * df[volt_str]
            col_name =  ivt_str + string_1[0:6] + amps_2[k] + '_dcpower'
            dfx[col_name] = dcpower
del df


#### Inverter Level DC Power Calculations from String DC Powers
ivt = ['IVT-1', 'IVT-2', 'IVT-3']
str_nums_1 = ['01','02','03','04','05','07','09','11','13','15','17','18','19','20']
str_nums_2 = ['01','03','04','05','07','08']
string_1 = 'String'
string_2 = 'dcpower'

tot_dcpower = pd.DataFrame()
for i in range(0, 3):
    dcpower = 0
    ivt_str = ivt[i] + '_' + 'String'
    IVT_STR = ivt[i] + '_' + 'dcpower'
    IVT_STR_thresh = ivt[i] + '_' + 'dcpower' + '_' + 'Thresh'
    
    if i < 1:
        for j in range(0, len(str_nums_1)):
            str_power = ivt_str + str_nums_1[j] + '_' + 'dcpower'
            tot_dcpower = dcpower + dfx[str_power]
            tot_dcpower_thresh = 0.85 * tot_dcpower
            dcpower = tot_dcpower
            
        dfx[IVT_STR] = tot_dcpower / 1000
        dfx[IVT_STR_thresh] = tot_dcpower_thresh / 1000
        
    else:
        for j in range(0, len(str_nums_2)):
            str_power = ivt_str + str_nums_2[j] + '_' + 'dcpower'
            tot_dcpower = dcpower + dfx[str_power]
            tot_dcpower_thresh = 0.85 * tot_dcpower
            dcpower = tot_dcpower
            
        dfx[IVT_STR] = tot_dcpower / 1000
        dfx[IVT_STR_thresh] = tot_dcpower_thresh / 1000
del tot_dcpower, dcpower, tot_dcpower_thresh


#### Dropping String Amp & Volt Tags
ivt = ['IVT-1', 'IVT-2', 'IVT-3']
str_idx_1 = ['01','02','03','04','05','07','09','11','13','15','17','18','19','20']
str_idx_2 = ['01','03','04','05','07','08']
string_1 = 'StringAmps'
string_2 = 'StringVolt'

for i in range(0, 3):
    ivt_str = ivt[i] + '_'
    
    if i < 1:
        for j in range(0, len(str_idx_1)):
            amp_str = ivt_str + string_1 + str_idx_1[j]
            volt_str = ivt_str + string_2 + str_idx_1[j]
            dfx.drop(amp_str, axis=1, inplace=True)
            dfx.drop(volt_str, axis=1, inplace=True)
    else:
        for j in range(0, len(str_idx_2)):
            amp_str = ivt_str + string_1 + str_idx_2[j]
            volt_str = ivt_str + string_2 + str_idx_2[j]
            dfx.drop(amp_str, axis=1, inplace=True)
            dfx.drop(volt_str, axis=1, inplace=True)


#### Constants & Variables
module = [252, 99, 99]
num_active_str = [14, 6, 6]
Module_power = 540
Gstc = 1000       # irradiance at standard test conditions
# Pstc =          # Number of module * Module power
delta = -0.35     # temperature coefficient for power(%/C, megative in sign) that corresponds to the installed modules
cor_fact = 0.985  # Factor of correction for lose
# Gpoa =          # Instant irradiance
Pstc_IVT_1 = module[0] * Module_power
Pstc_IVT_2 = module[1] * Module_power
Pstc_IVT_3 = module[2] * Module_power

Pstc_str_IVT_1 = (module[0]/num_active_str[0]) * Module_power
Pstc_str_IVT_2 = (module[1]/num_active_str[1]) * Module_power
Pstc_str_IVT_3 = (module[2]/num_active_str[2]) * Module_power

tot_irrad_1 = dfx['MET-1_HalfCellRad1'].sum()
tot_irrad_2 = dfx['MET-1_HalfCellRad1'].sum()

Gpoa_1 = tot_irrad_1/1000 # in kW/m2
Gpoa_2 = tot_irrad_2/1000 # in kW/m2
Gpoa_avg = (Gpoa_1 + Gpoa_2) / 2

# 2. Expected Power DC = ([cor_fact * Pstc * (Gpoa/Gstc)] * (1 + (delta/100) * (CellTemperature - 25))) / 1000
# 3. Voltage Drop AC/DC = 100 - (KWAC * 100)/KWDC
# 4. AC/DC Ratio = KWAC/KWDC
# 5. DC Ratio = KWDC/Expected Power DC


#### Plant Expected Power
## Expected Power DC = ([cor_fact * Pstc * (Gpoa/Gstc)] * (1 + (delta/100) * (CellTemperature - 25))) / 1000
dfx['IVT-1_Expected_power'] = (cor_fact * Pstc_IVT_1 * (dfx['MET-1_HalfCellRad1']/Gstc) * (1 + (delta/100) * (dfx['MET-1_PanelTemp'] - 25)))/1000
dfx['IVT-2_Expected_power'] = (cor_fact * Pstc_IVT_2 * (dfx['MET-1_HalfCellRad1']/Gstc) * (1 + (delta/100) * (dfx['MET-1_PanelTemp'] - 25)))/1000
dfx['IVT-3_Expected_power'] = (cor_fact * Pstc_IVT_3 * (dfx['MET-1_HalfCellRad1']/Gstc) * (1 + (delta/100) * (dfx['MET-1_PanelTemp'] - 25)))/1000

# Threshold Calculation
dfx['IVT-1_Expected_thresh'] = 0.85 * dfx['IVT-1_Expected_power']
dfx['IVT-2_Expected_thresh'] = 0.85 * dfx['IVT-2_Expected_power']
dfx['IVT-3_Expected_thresh'] = 0.85 * dfx['IVT-3_Expected_power']


#### String Expected Power
dfx['IVT-1_Str_Expected_power'] = (cor_fact * Pstc_str_IVT_1 * (dfx['MET-1_HalfCellRad1']/Gstc) * (1 + (delta/100) * (dfx['MET-1_PanelTemp'] - 25)))
dfx['IVT-2_Str_Expected_power'] = (cor_fact * Pstc_str_IVT_2 * (dfx['MET-1_HalfCellRad1']/Gstc) * (1 + (delta/100) * (dfx['MET-1_PanelTemp'] - 25)))
dfx['IVT-3_Str_Expected_power'] = (cor_fact * Pstc_str_IVT_3 * (dfx['MET-1_HalfCellRad1']/Gstc) * (1 + (delta/100) * (dfx['MET-1_PanelTemp'] - 25)))

# Threshold Calculation
dfx['IVT-1_Str_Expected_thresh'] = 0.85 * dfx['IVT-1_Str_Expected_power']
dfx['IVT-2_Str_Expected_thresh'] = 0.85 * dfx['IVT-2_Str_Expected_power']
dfx['IVT-3_Str_Expected_thresh'] = 0.85 * dfx['IVT-3_Str_Expected_power']

# Irradiance Correction
df1 = dfx.copy()
df1['MET-1_HalfCellRad1_mod'] = dfx['MET-1_HalfCellRad1'].apply(lambda x: 0 if x < 10 else x)

# df1['Max_Rad'] = df1[['MET-1_HalfCellRad1_mod', 'MET-2_HalfCellRad1_mod']].max(axis=1)
df1['Max_Rad'] = df1['MET-1_HalfCellRad1_mod']
df1['Effective_Rad'] = df1['Max_Rad'].apply(lambda x: 1 if x >= 10 else 0)
df1.drop('MET-1_HalfCellRad1_mod', axis=1, inplace=True)
del dfx


#### Voltage Drop, AC/DC Ratio, and DC Ratio 
## Voltage Drop AC/DC = 100 - (KWAC * 100)/KWDC

dfx = df1.copy()

dfx['IVT-1_KWAC_effective'] = dfx['IVT-1_KWAC'] * df1['Effective_Rad']
dfx['IVT-2_KWAC_effective'] = dfx['IVT-2_KWAC'] * df1['Effective_Rad'] 
dfx['IVT-3_KWAC_effective'] = dfx['IVT-3_KWAC'] * df1['Effective_Rad'] 

dfx['IVT-1_KWDC_effective'] = dfx['IVT-1_KWDC'] * df1['Effective_Rad']
dfx['IVT-2_KWDC_effective'] = dfx['IVT-2_KWDC'] * df1['Effective_Rad'] 
dfx['IVT-3_KWDC_effective'] = dfx['IVT-3_KWDC'] * df1['Effective_Rad'] 

dfx['IVT-1_AC/DC_ratio'] = (dfx['IVT-1_KWAC_effective'] / dfx['IVT-1_KWDC_effective'])*100
dfx['IVT-2_AC/DC_ratio'] = (dfx['IVT-2_KWAC_effective'] / dfx['IVT-2_KWDC_effective'])*100
dfx['IVT-3_AC/DC_ratio'] = (dfx['IVT-3_KWAC_effective'] / dfx['IVT-3_KWDC_effective'])*100

dfx['IVT-1_AC/DC_ratio'] = dfx['IVT-1_AC/DC_ratio'].apply(lambda x: 100 if x > 100 else x)
dfx['IVT-2_AC/DC_ratio'] = dfx['IVT-2_AC/DC_ratio'].apply(lambda x: 100 if x > 100 else x)
dfx['IVT-3_AC/DC_ratio'] = dfx['IVT-3_AC/DC_ratio'].apply(lambda x: 100 if x > 100 else x)

dfx['Effective_Rad'] = df1['Effective_Rad'] * 100
dfx['Effective_Rad_half'] = dfx['Effective_Rad'] * 0.4
del df1

df1 = dfx.copy()
df1['IVT-1_AC/DC_ratio'] = dfx['IVT-1_AC/DC_ratio'].apply(lambda x: 100 if x > 100 else x)
df1['IVT-2_AC/DC_ratio'] = dfx['IVT-2_AC/DC_ratio'].apply(lambda x: 100 if x > 100 else x)
df1['IVT-3_AC/DC_ratio'] = dfx['IVT-3_AC/DC_ratio'].apply(lambda x: 100 if x > 100 else x)
df1 = dfx.fillna(0)
df1.replace([np.inf, -np.inf], 0, inplace=True)

df1['IVT-1_AC/DC_ratio'] = df1['IVT-1_AC/DC_ratio'].apply(lambda x: 100 if x == 0 else x)
df1['IVT-2_AC/DC_ratio'] = df1['IVT-2_AC/DC_ratio'].apply(lambda x: 100 if x == 0 else x)
df1['IVT-3_AC/DC_ratio'] = df1['IVT-3_AC/DC_ratio'].apply(lambda x: 100 if x == 0 else x)
del dfx


#### AC/DC Ratio Drop and Threshold at 4% Below
## DC Ratio = KWDC/Expected Power DC
# 1. convert zero to 100: dfx['IVT-1_AC/DC_ratio']
# 2. Calculate dfx['IVT-1_AC/DC_ratio_drop']
# 3. Convert negative values to zero

dfx = df1.copy()

dfx['IVT-1_AC/DC_ratio_drop'] = 100 - df1['IVT-1_AC/DC_ratio']
dfx['IVT-2_AC/DC_ratio_drop'] = 100 - df1['IVT-2_AC/DC_ratio']
dfx['IVT-3_AC/DC_ratio_drop'] = 100 - df1['IVT-3_AC/DC_ratio']
del df1

dfx = dfx.fillna(0)
dfx.replace([np.inf, -np.inf], 0, inplace=True)


#### Save to a Desination Folder
dfx.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/SHO_Daily_ConditionMonitoring.csv', index=False, float_format='%.2f')