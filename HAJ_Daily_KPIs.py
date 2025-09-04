#!/usr/bin/env python
# coding: utf-8

# HAJIME Daily KPI Calculations
# Two (2) Inverters

# Daily KPI calculations- 08/25/2022, Code modified for String tag list changes
# Daily KPI calculations- 11/14/2022, Code modified to reflect Daylight Savings Time Error


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


#### Reading Tag Lists
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='HAJ_daily')
dim = len(df_list)


#### Setting Date, finding yesterday's date
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


#### SQL Query
## SQL Configuration
cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Abbrev_Name'])
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

# Date Correction
Date_1 = datetime.strptime(str(start_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
Date_2 = datetime.strptime(str(end_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# Remove Exceeding Date
df1 = dfx[dfx['TimeStamp'] >= str(Date_1)]
df2 = df1[df1['TimeStamp'] <= str(Date_2)]
df = df2.copy()
del df1, df2, df_list, dfx


#### Saving Daily Queried Data
df.to_csv('HAJ_Daily_KPI_Query.csv', index=False)


#### KPI Calculation
## Removing NaNs
df = df.fillna(0)

## Calendar
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
df['date'] = df['TimeStamp'].dt.date
df['year'] = df['TimeStamp'].dt.year
df['quarter'] = df['TimeStamp'].dt.quarter
df['month'] = df['TimeStamp'].dt.month
df['week'] = df['TimeStamp'].dt.isocalendar().week
df['day'] = df['TimeStamp'].dt.day
df['hour'] = df['TimeStamp'].dt.hour
# Move 'DateTime' column into front column
Hour = df['hour']
df.drop(labels=['hour'], axis=1, inplace=True)
df.insert(1,'Hour', Hour)
# Move 'DateTime' column into front column
Date = df['day']
df.drop(labels=['day'], axis=1, inplace=True)
df.insert(1,'Day', Date)
# Move 'DateTime' column into front column
Date = df['week']
df.drop(labels=['week'], axis=1, inplace=True)
df.insert(1,'Week', Date)
# Move 'DateTime' column into front column
Date = df['month']
df.drop(labels=['month'], axis=1, inplace=True)
df.insert(1,'Month', Date)
# Move 'DateTime' column into front column
Date = df['quarter']
df.drop(labels=['quarter'], axis=1, inplace=True)
df.insert(1,'Quarter', Date)
# Move 'DateTime' column into front column
Date = df['year']
df.drop(labels=['year'], axis=1, inplace=True)
df.insert(1,'Year', Date)
# Move 'DateTime' column into front column
Date = df['date']
df.drop(labels=['date'], axis=1, inplace=True)
df.insert(1,'Date', Date)

## Converting Data Type
# Listing 'object' columns
obj_cols = []
for i in df.columns:
    if df[i].dtype == 'object':
        obj_cols.append(i)

# Converting 'object' data type to 'float' data type
for i in range(len(obj_cols)):
    if i != 0:
        df[obj_cols[i]] = pd.to_numeric(df[obj_cols[i]], errors='coerce')


#### Time Based Availabilty, TBA
df['MET-1_Irrad_Totalizer'] = df['MET-1_HalfCellRad1']

df1 = df[df['MET-1_HalfCellRad1'] > 20]
ivt_1_down = df1[df1['IVT-1_KWDC'] < 3.0] #ivt_1_down = df1[df1['IVT-1_KWDC'] < 0.1]
ivt_2_down = df1[df1['IVT-2_KWDC'] < 3.0] #ivt_2_down = df1[df1['IVT-2_KWDC'] < 0.1]

TBA_ivt_1 = (1-(len(ivt_1_down)/len(df1['MET-1_HalfCellRad1']))) * 100
TBA_ivt_2 = (1-(len(ivt_2_down)/len(df1['MET-1_HalfCellRad1']))) * 100
TBA_ivt = [TBA_ivt_1, TBA_ivt_2]
TBA_plant = sum(TBA_ivt)/len(TBA_ivt)
del df1

current_date = str(df['Date'].values[0])
result = {'Date': current_date, 'TBA_Plant': [TBA_plant], 'TBA_IVT_1': [TBA_ivt_1], 'TBA_IVT_2': [TBA_ivt_2]}
KPIs = pd.DataFrame(result, columns = ['Date', 'TBA_Plant', 'TBA_IVT_1', 'TBA_IVT_2'])


#### Performance Ratio, Inverter Level
# Some Constants
Gstc = 1000 # irradiance at standard test conditions
module = [278, 278]
delta_temp_coeff = -0.35 # temperature coefficient for power(%/C, megative in sign) that corresponds to the installed modules
Tcell_typ_avg_1 = 65.97 # average cell temperature computed from one year of weather data using the project weather file
Tcell_typ_avg_2 = 61.68
Tcell_typ_avg = (Tcell_typ_avg_1 + Tcell_typ_avg_1) / 2

numer_IVT_1 = df['IVT-1_KWAC'].sum()
numer_IVT_2 = df['IVT-2_KWAC'].sum()

module_pk_pwr = 455
Pstc_IVT_1 = module[0] * module_pk_pwr
Pstc_IVT_2 = module[1] * module_pk_pwr

tot_irrad_1 = df['MET-1_HalfCellRad1'].sum()
Gpoa_1 = tot_irrad_1 / 1000
Gpoa_avg = Gpoa_1

# For Inverter
denom_IVT_1 = Pstc_IVT_1 * (Gpoa_1 / Gstc)
denom_IVT_2 = Pstc_IVT_2 * (Gpoa_1 / Gstc)

PR_ivt_1 = (numer_IVT_1 / denom_IVT_1) * 100
PR_ivt_2 = (numer_IVT_2 / denom_IVT_2) * 100
PR_plant = sum([PR_ivt_1, PR_ivt_2])/len([PR_ivt_1, PR_ivt_2])

result = {'Date': current_date, 'PR_Plant': [PR_plant], 'PR_IVT_1': [PR_ivt_1], 'PR_IVT_2': [PR_ivt_2]}
PR = pd.DataFrame(result, columns = ['Date', 'PR_Plant', 'PR_IVT_1', 'PR_IVT_2'])
KPIs = pd.merge(KPIs, PR, how='outer', on=['Date','Date'])


#### Weather Corrected Performance Ratio, Inverter Level
Tcell_1_max = df['MET-1_PanelTemp'].max()

denom_IVT_1_adj = (Pstc_IVT_1 * (Gpoa_1 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))
denom_IVT_2_adj = (Pstc_IVT_2 * (Gpoa_1 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))

PR_ivt_1_adj = (numer_IVT_1 / denom_IVT_1_adj) * 100
PR_ivt_2_adj = (numer_IVT_2 / denom_IVT_2_adj) * 100
PR_plant_adj = sum([PR_ivt_1_adj, PR_ivt_2_adj])/len([PR_ivt_1_adj, PR_ivt_2_adj])

result = {'Date':current_date, 'PR_Plant_Weather':[PR_plant_adj], 'PR_IVT_1_Weather':[PR_ivt_1_adj], 'PR_IVT_2_Weather':[PR_ivt_2_adj]}
PR_Weather = pd.DataFrame(result, columns = ['Date', 'PR_Plant_Weather', 'PR_IVT_1_Weather', 'PR_IVT_2_Weather'])
KPIs = pd.merge(KPIs, PR_Weather, how='outer', on=['Date','Date'])


#### Performance Ratio, String Level
# DC Power Calculation
ivt = ['IVT-1', 'IVT-2']
amps_ivt_1 = ['01','02','03','04','05','06','08','10','12','14','16','17','18','19','20']
volts_ivt_1 = ['01','02','03','04','05','06','08','10','12','14','16','17','18','19','20']
amps_ivt_2 = ['01','02','03','04','05','06','08','10','12','14','16','17','18','19','20']
volts_ivt_2 = ['01','02','03','04','05','06','08','10','12','14','16','17','18','19','20']
string_1 = 'StringAmps'
string_2 = 'StringVolt'

dcpower_ivt_1 = pd.DataFrame()
dcpower_ivt_2 = pd.DataFrame()
for i in range(0,2):
    ivt_str = ivt[i] + '_'
    
    if i+1 == 1:  
        for j in range(len(amps_ivt_1)):
            amp_str = ivt_str + string_1 + amps_ivt_1[j]
            volt_str = ivt_str + string_2 + volts_ivt_1[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps_ivt_1[j]
            df[col_name] = dcpower
        del amp_str, volt_str, dcpower, col_name
        
    if i+1 == 2:
        for j in range(len(amps_ivt_2)):
            amp_str = ivt_str + string_1 + amps_ivt_2[j]
            volt_str = ivt_str + string_2 + volts_ivt_2[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps_ivt_2[j]
            df[col_name] = dcpower
        del amp_str, volt_str, dcpower, col_name

## PDCstc Value for Each String
dc_pwr_str = df.iloc[0:, 80:].sum()

# Two Inverters
PDCstc_ivt_1 = [8645,8645,8645,8645,8645,8645,8190,8190,8190,8190,8190,8645,8645,8645,8645]
PDCstc_ivt_2 = [8645,8645,8645,8645,8645,8645,8190,8190,8190,8190,8190,8645,8645,8645,8645]

## String PR
col_name_list = df.iloc[0:, 80:].columns.values.tolist()

PR_str_name = []
PR_str_val = []
for k in range (0, len(col_name_list)):
    PR_str_name.append(col_name_list[k].replace('dcpower', 'PR'))
    
    if k < 15:
        str_PR = dc_pwr_str[k] / (PDCstc_ivt_1[k] * (tot_irrad_1 / Gstc))
        
    elif (k >= 15):
        n = k - 15
        str_PR = dc_pwr_str[k] / (PDCstc_ivt_2[n] * (tot_irrad_1 / Gstc))
    
    PR_str_val.append('{:.2f}'.format(str_PR))


#### Soiling Loss
## Soiling Formula-1 used
IVT_1_soiling = []
IVT_2_soiling = []
for j in range(0, len(PR_str_name)):
    if PR_str_name[j].find('IVT-1') != -1:       
        soiling = abs(100 - ((float(PR_str_val[j]) * 100)/0.9))
        if soiling == 100.0:
            soiling = 0.0
        IVT_1_soiling.append('{:.2f}'.format(soiling))
    
    elif PR_str_name[j].find('IVT-2') != -1:       
        soiling = abs(100 - ((float(PR_str_val[j]) * 100)/0.9))
        if soiling == 100.0:
            soiling = 0.0
        IVT_2_soiling.append('{:.2f}'.format(soiling))
          
## Soiling Loss- Inverter Level
# Converting Strings to Floating
IVT_1_soiling = list(map(float, IVT_1_soiling))
IVT_2_soiling = list(map(float, IVT_2_soiling))

# Soiling Loss- Inverter Level
KPIs['Soiling_Loss_IVT_1'] = mean(IVT_1_soiling)
KPIs['Soiling_Loss_IVT_2'] = mean(IVT_2_soiling)
KPIs['Soiling_Loss_Plant'] = (KPIs['Soiling_Loss_IVT_1'] + KPIs['Soiling_Loss_IVT_2']) / 2


#### Importing Projection Irradiance and Production
# Current Month to Figure the Previous Month
Current_Month = df['Month'].iloc[-1]

# Read Projected Production & Irradiance Number from a Reference File
Projected_Production = [29.58, 32.43, 40.60, 37.73, 41.33, 37.66, 38.00, 37.20, 32.18, 33.02, 31.44, 28.19] # in MWh
Projected_Irradiance = [137.1, 155.2, 202.1, 210.6, 213.4, 193.7, 191.9, 186.6, 159.6, 160.5, 148.1, 129.8] # in kW/m2
KPIs['Projected_Production_last month'] = Projected_Production[Current_Month-2] # Last month's value
KPIs['Projected_Irradiance_last month'] = Projected_Irradiance[Current_Month-2] # Last month's value


#### Reading Past Month KPIs
df_past = pd.read_csv('KIWA_past_month_kpis_monthly.csv')

# Actual Production, Past Month
KPIs['Actual_Prod_last_month'] = df_past['HAJ_Past_Month_Prod']

# Ratio of Production, Actual & Projected
KPIs['Ratio_Prod_ActualvsProject'] = df_past['HAJ_Past_Month_Prod'] / KPIs['Projected_Production_last month'] 

# Actual Irradiation, Past Month
KPIs['Actual_Irrad_last_month'] = df_past['HAJ_Past_Month_Irrad']

# Ratio of Irradiation, Actual & Projected
KPIs['Ratio_Irrad_ActualvsProject'] = df_past['HAJ_Past_Month_Irrad'] / KPIs['Projected_Irradiance_last month']


#### Saving Results
KPIs.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaDataExport/HAJ_kpis.csv', index=False, float_format='%.2f')

