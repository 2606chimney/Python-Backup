#!/usr/bin/env python
# coding: utf-8

## MURAKAMI Daily KPI Calculations- 11/18/2022

# 6- inverters, 24- amps, 24-volts, 2- MET stations, MET-1 Cell-1 & MET-2 Cell-1, MET-1 Panel Temp & MET-2 Panel temp
# Modified for Daylight Saving Time and Query Efficiency Improvement- 11/18/2022
# Modified with PDCstc Values- 12/13/2022


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
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='MUR_daily')
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
df.to_csv('MUR_Daily_KPI_Query.csv', index=False)


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


#### Time Based Availability
# df['MET-1_Irrad_Totalizer'] = df[['MET-1_HalfCellRad1', 'MET-1_HalfCellRad2']].max(axis=1)
# df['MET-2_Irrad_Totalizer'] = df[['MET-2_HalfCellRad1', 'MET-2_HalfCellRad2']].max(axis=1)
df['MET-1_Irrad_Totalizer'] = df['MET-1_HalfCellRad1']
df['MET-2_Irrad_Totalizer'] = df['MET-2_HalfCellRad1']

df1 = df[df['MET-1_Irrad_Totalizer'] > 0]
df2 = df[df['MET-2_Irrad_Totalizer'] > 0]

# Decide a cutoff threshold value- Inverter Down
ivt_1_down = df1[df1['IVT-1_KWAC'] < 1.0]
ivt_2_down = df1[df1['IVT-2_KWAC'] < 1.0]
ivt_3_down = df2[df2['IVT-3_KWAC'] < 1.0]
ivt_4_down = df2[df2['IVT-4_KWAC'] < 1.0]
ivt_5_down = df2[df2['IVT-5_KWAC'] < 1.0]
ivt_6_down = df2[df2['IVT-6_KWAC'] < 1.0]

# Inverter Up
ivt_1_up = df1[df1['IVT-1_KWAC'] > 0]
ivt_2_up = df1[df1['IVT-2_KWAC'] > 0]
ivt_3_up = df2[df2['IVT-3_KWAC'] > 0]
ivt_4_up = df2[df2['IVT-4_KWAC'] > 0]
ivt_5_up = df2[df2['IVT-5_KWAC'] > 0]
ivt_6_up = df2[df2['IVT-6_KWAC'] > 0]

# Time Based Availability
TBA_ivt_1 = (1-(len(ivt_1_down)/len(df1['MET-1_HalfCellRad1']))) * 100
TBA_ivt_2 = (1-(len(ivt_2_down)/len(df1['MET-1_HalfCellRad1']))) * 100
TBA_ivt_3 = (1-(len(ivt_3_down)/len(df2['MET-2_HalfCellRad1']))) * 100
TBA_ivt_4 = (1-(len(ivt_4_down)/len(df2['MET-2_HalfCellRad1']))) * 100
TBA_ivt_5 = (1-(len(ivt_5_down)/len(df2['MET-2_HalfCellRad1']))) * 100
TBA_ivt_6 = (1-(len(ivt_6_down)/len(df2['MET-2_HalfCellRad1']))) * 100

TBA_ivt = [TBA_ivt_1, TBA_ivt_2, TBA_ivt_3, TBA_ivt_4, TBA_ivt_5, TBA_ivt_6]
TBA_plant = sum(TBA_ivt)/len(TBA_ivt)
del df1, df2

current_date = str(df['Date'].values[0])
result = {'Date':current_date,'TBA_Plant':[TBA_plant],'TBA_IVT_1':[TBA_ivt_1],'TBA_IVT_2':[TBA_ivt_2],'TBA_IVT_3':[TBA_ivt_3],
          'TBA_IVT_4':[TBA_ivt_4],'TBA_IVT_5':[TBA_ivt_5],'TBA_IVT_6':[TBA_ivt_6]}
KPIs = pd.DataFrame(result, columns = ['Date','TBA_Plant','TBA_IVT_1','TBA_IVT_2','TBA_IVT_3','TBA_IVT_4','TBA_IVT_5','TBA_IVT_6'])


#### Performance Ratio, Inverter Level
# Some Constants
Gstc = 1000 # irradiance at standard test conditions
module = [96, 96, 250, 250, 250, 250]
delta_temp_coeff = -0.34 # temperature coefficient for power(%/C, megative in sign) that corresponds to the installed modules
Tcell_typ_avg_1 = 65.97 # average cell temperature computed from one year of weather data using the project weather file
Tcell_typ_avg_2 = 61.68

numer_IVT_1 = df['IVT-1_KWAC'].sum()
numer_IVT_2 = df['IVT-2_KWAC'].sum()
numer_IVT_3 = df['IVT-3_KWAC'].sum()
numer_IVT_4 = df['IVT-4_KWAC'].sum()
numer_IVT_5 = df['IVT-5_KWAC'].sum()
numer_IVT_6 = df['IVT-6_KWAC'].sum()

module_pk_pwr = 545
Pstc_IVT_1 = module[0] * module_pk_pwr
Pstc_IVT_2 = module[1] * module_pk_pwr
Pstc_IVT_3 = module[2] * module_pk_pwr
Pstc_IVT_4 = module[3] * module_pk_pwr
Pstc_IVT_5 = module[4] * module_pk_pwr
Pstc_IVT_6 = module[5] * module_pk_pwr

tot_irrad_1 = df['MET-1_Irrad_Totalizer'].sum()
tot_irrad_2 = df['MET-2_Irrad_Totalizer'].sum()

Gpoa_1 = tot_irrad_1/1000 # in kW/m2
Gpoa_2 = tot_irrad_2/1000 # in kW/m2

# For Inverter
denom_IVT_1 = Pstc_IVT_1 * (Gpoa_1 / Gstc)
denom_IVT_2 = Pstc_IVT_2 * (Gpoa_1 / Gstc)
denom_IVT_3 = Pstc_IVT_3 * (Gpoa_2 / Gstc)
denom_IVT_4 = Pstc_IVT_4 * (Gpoa_2 / Gstc)
denom_IVT_5 = Pstc_IVT_5 * (Gpoa_2 / Gstc)
denom_IVT_6 = Pstc_IVT_6 * (Gpoa_2 / Gstc)

PR_ivt_1 = (numer_IVT_1 / denom_IVT_1) * 100
PR_ivt_2 = (numer_IVT_2 / denom_IVT_2) * 100
PR_ivt_3 = (numer_IVT_3 / denom_IVT_3) * 100
PR_ivt_4 = (numer_IVT_4 / denom_IVT_4) * 100
PR_ivt_5 = (numer_IVT_5 / denom_IVT_5) * 100
PR_ivt_6 = (numer_IVT_6 / denom_IVT_6) * 100
PR_plant = sum([PR_ivt_1,PR_ivt_2,PR_ivt_3,PR_ivt_4,PR_ivt_5,PR_ivt_6])/len([PR_ivt_1,PR_ivt_2,PR_ivt_3,PR_ivt_4,PR_ivt_5,PR_ivt_6])

result = {'Date':current_date,'PR_Plant':[PR_plant],'PR_IVT_1':[PR_ivt_1],'PR_IVT_2':[PR_ivt_2],'PR_IVT_3':[PR_ivt_3],
          'PR_IVT_4':[PR_ivt_4],'PR_IVT_5':[PR_ivt_5],'PR_IVT_6':[PR_ivt_6]}
PR = pd.DataFrame(result, columns = ['Date','PR_Plant','PR_IVT_1','PR_IVT_2','PR_IVT_3','PR_IVT_4','PR_IVT_5','PR_IVT_6'])
KPIs = pd.merge(KPIs, PR, how='outer', on=['Date','Date'])


#### Weather Corrected Performance Ratio, Inverter Level
Tcell_1_max = df['MET-1_PanelTemp'].max()
Tcell_2_max = df['MET-2_PanelTemp'].max()

denom_IVT_1_adj = (Pstc_IVT_1 * (Gpoa_1 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))
denom_IVT_2_adj = (Pstc_IVT_2 * (Gpoa_1 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))
denom_IVT_3_adj = (Pstc_IVT_3 * (Gpoa_2 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_2 - Tcell_2_max))
denom_IVT_4_adj = (Pstc_IVT_4 * (Gpoa_2 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_2 - Tcell_2_max))
denom_IVT_5_adj = (Pstc_IVT_5 * (Gpoa_2 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_2 - Tcell_2_max))
denom_IVT_6_adj = (Pstc_IVT_6 * (Gpoa_2 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_2 - Tcell_2_max))

PR_ivt_1_adj = (numer_IVT_1 / denom_IVT_1_adj) * 100
PR_ivt_2_adj = (numer_IVT_2 / denom_IVT_2_adj) * 100
PR_ivt_3_adj = (numer_IVT_3 / denom_IVT_3_adj) * 100
PR_ivt_4_adj = (numer_IVT_4 / denom_IVT_4_adj) * 100
PR_ivt_5_adj = (numer_IVT_5 / denom_IVT_5_adj) * 100
PR_ivt_6_adj = (numer_IVT_6 / denom_IVT_6_adj) * 100
PR_plant_adj = sum([PR_ivt_1_adj,PR_ivt_2_adj,PR_ivt_3_adj,PR_ivt_4_adj,PR_ivt_5_adj,PR_ivt_6_adj])/len([PR_ivt_1_adj,PR_ivt_2_adj,PR_ivt_3_adj,PR_ivt_4_adj,PR_ivt_5_adj,PR_ivt_6_adj])

result = {'Date': current_date, 'PR_Plant_Weather': [PR_plant_adj], 'PR_IVT_1_Weather': [PR_ivt_1_adj],
          'PR_IVT_2_Weather': [PR_ivt_2_adj], 'PR_IVT_3_Weather': [PR_ivt_3_adj], 'PR_IVT_4_Weather': [PR_ivt_4_adj],
          'PR_IVT_5_Weather': [PR_ivt_5_adj], 'PR_IVT_6_Weather': [PR_ivt_6_adj]}
PR_Weather = pd.DataFrame(result, columns = ['Date', 'PR_Plant_Weather', 'PR_IVT_1_Weather', 'PR_IVT_2_Weather',
                                             'PR_IVT_3_Weather','PR_IVT_4_Weather', 'PR_IVT_5_Weather', 'PR_IVT_6_Weather'])
KPIs = pd.merge(KPIs, PR_Weather, how='outer', on=['Date','Date'])


#### Performance Ratio, String Level
# DC Power Calculation
ivt = ['IVT-1', 'IVT-2', 'IVT-3', 'IVT-4', 'IVT-5', 'IVT-6']
amps = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24']
volts = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24']
string_1 = 'StringAmps'
string_2 = 'StringVolt'

for i in range(0, 6):
    ivt_str = ivt[i] + '_'

    if i == 0:  
        for j in range(len(amps)):
            amp_str = ivt_str + string_1 + amps[j]
            volt_str = ivt_str + string_2 + volts[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps[j]
            df[col_name] = dcpower
        del amp_str, volt_str, dcpower, col_name
        
    if i == 1:
        for j in range(len(amps)):
            frame = df.copy()
            amp_str = ivt_str + string_1 + amps[j]
            volt_str = ivt_str + string_2 + volts[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps[j]
            frame[col_name] = dcpower
            df = frame.copy()
        del amp_str, volt_str, dcpower, col_name
        
    if i == 2:
        for j in range(len(amps)):
            frame = df.copy()
            amp_str = ivt_str + string_1 + amps[j]
            volt_str = ivt_str + string_2 + volts[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps[j]
            frame[col_name] = dcpower
            df = frame.copy()
        del amp_str, volt_str, dcpower, col_name
        
    if i == 3:
        for j in range(len(amps)):
            frame = df.copy()
            amp_str = ivt_str + string_1 + amps[j]
            volt_str = ivt_str + string_2 + volts[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps[j]
            frame[col_name] = dcpower
            df = frame.copy()
        del amp_str, volt_str, dcpower, col_name
        
    if i == 4:
        for j in range(len(amps)):
            frame = df.copy()
            amp_str = ivt_str + string_1 + amps[j]
            volt_str = ivt_str + string_2 + volts[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps[j]
            frame[col_name] = dcpower
            df = frame.copy()
        del amp_str, volt_str, dcpower, col_name
        
    if i == 5:
        for j in range(len(amps)):
            frame = df.copy()
            amp_str = ivt_str + string_1 + amps[j]
            volt_str = ivt_str + string_2 + volts[j]
            dcpower = df[amp_str] * df[volt_str]
            col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps[j]
            frame[col_name] = dcpower
            df = frame.copy()
        del amp_str, volt_str, dcpower, col_name
del frame

## Total Power, String Level
dcpower_numeric = df.iloc[0:, 324:].sum()

## PDCstc, Inverter Level
PDCstc = [13080, 13080, 13625, 13625, 13625, 13625]
PDCstc_ivt_1 = [13080,0,13080,0,13080,0,13080,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
PDCstc_ivt_2 = [13080,0,13080,0,13080,0,13080,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
PDCstc_ivt_3 = [0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,0,0,0]
PDCstc_ivt_4 = [0,13500,0,13500,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,0,0,0]
PDCstc_ivt_5 = [0,13500,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,0,0,0]
PDCstc_ivt_6 = [0,13500,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,13625,0,0,0,0]


#### String PR
num_string = 24
PDCstc_invt_num = [PDCstc_ivt_1, PDCstc_ivt_2, PDCstc_ivt_3, PDCstc_ivt_4, PDCstc_ivt_5, PDCstc_ivt_6]

for i in range(0, 6):
    PDC_idx = PDCstc_invt_num[i]
    s = i + 1
    new_str = 'PR_' + ivt[i][0:3] + '_' + str(s)
    
    PR_string = []
    for j in range(0, num_string):
        new_col_name = new_str + '_' + string_1[0:6] + amps[j]
        pre_idx = s*num_string-num_string
        
        if PDC_idx[j] == 0:
            new_col_name = 0
        else:
            if s < 3:
                new_col_name = dcpower_numeric[pre_idx+j] / (PDC_idx[j] * (tot_irrad_1 / Gstc))
            else:
                new_col_name = dcpower_numeric[pre_idx+j] / (PDC_idx[j] * (tot_irrad_2 / Gstc))
        
        PR_string.append("{:.2f}".format(new_col_name))
        
## For String PR Print Out ##    
    if i == 0:
        PR_IVT_STR = pd.DataFrame(PR_string)
        PR_IVT_STR.columns = [new_str]
    else:
        PR_IVT_STR[new_str] = PR_string

# Converting Data Type
PR_IVT_STR = PR_IVT_STR.astype(float)


#### Soiling Loss
## Soiling Formula-1
num_inverter = 6

for i in range(0, num_inverter):
    s = i + 1
    ivt_num = 'PR_IVT_' + str(i+1)
    
    Soiling_string = []
    for j in range(0, num_string):
        column_name = 'IVT_' + str(i+1) + '_Soiling_Loss'
        soiling_1 = 100 - ((PR_IVT_STR[ivt_num][j] * 100)/0.9)
        if soiling_1 == 100.0:
            soiling_1 = 0.0
            
        soiling_2 = (PR_IVT_STR[ivt_num][j] / (PR_IVT_STR[ivt_num].sum())) * 100
        Soiling_string.append("{:.2f}".format(soiling_1))
    
    if i == 0:
        Soiling_IVT = pd.DataFrame(Soiling_string)
        Soiling_IVT.columns = [column_name]
    else:
        Soiling_IVT[column_name] = Soiling_string

# Converting Data Type
Soiling_IVT = Soiling_IVT.astype(float)

## Soiling Loss- Inverter Level
# ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_1_Soiling_Loss']] != 0).all(axis=1)]
ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_1_Soiling_Loss']] > 0).all(axis=1)]
KPIs['Soiling_Loss_IVT_1'] = ivt_soiling['IVT_1_Soiling_Loss'].mean()

#ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_2_Soiling_Loss']] != 0).all(axis=1)]
ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_2_Soiling_Loss']] > 0).all(axis=1)]
KPIs['Soiling_Loss_IVT_2'] = ivt_soiling['IVT_2_Soiling_Loss'].mean()

#ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_3_Soiling_Loss']] != 0).all(axis=1)]
ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_3_Soiling_Loss']] > 0).all(axis=1)]
KPIs['Soiling_Loss_IVT_3'] = ivt_soiling['IVT_3_Soiling_Loss'].mean()

#ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_4_Soiling_Loss']] != 0).all(axis=1)]
ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_4_Soiling_Loss']] > 0).all(axis=1)]
KPIs['Soiling_Loss_IVT_4'] = ivt_soiling['IVT_4_Soiling_Loss'].mean()

#ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_5_Soiling_Loss']] != 0).all(axis=1)]
ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_5_Soiling_Loss']] > 0).all(axis=1)]
KPIs['Soiling_Loss_IVT_5'] = ivt_soiling['IVT_5_Soiling_Loss'].mean()

#ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_6_Soiling_Loss']] != 0).all(axis=1)]
ivt_soiling = Soiling_IVT[(Soiling_IVT[['IVT_6_Soiling_Loss']] > 0).all(axis=1)]
KPIs['Soiling_Loss_IVT_6'] = ivt_soiling['IVT_6_Soiling_Loss'].mean()

KPIs = KPIs.fillna(0)
    
KPIs['Soiling_Loss_Plant'] = (KPIs['Soiling_Loss_IVT_1'] + KPIs['Soiling_Loss_IVT_2'] + KPIs['Soiling_Loss_IVT_3'] +
                              KPIs['Soiling_Loss_IVT_4'] + KPIs['Soiling_Loss_IVT_5'] + KPIs['Soiling_Loss_IVT_6']) / 6


#### Current Month to Figure the Previous Month
Current_Month = df['Month'].iloc[-1]


#### Read Projected Production & Irradiance Number from a Reference File
Projected_Production = [76.0, 83.7, 109.5, 94.5, 111.4, 101.3, 99.7, 97.1, 83.3, 84.0, 80.4, 72.7] # in MWh
Projected_Irradiance = [137.0, 155.1, 209.9, 203.2, 215.8, 193.4, 188.5, 183.9, 156.0, 155.8, 147.6, 131.3] # in kWh/m2

KPIs['Projected_Production_last month'] = Projected_Production[Current_Month-2] # Last month's value
KPIs['Projected_Irradiance_last month'] = Projected_Irradiance[Current_Month-2] # Last month's value


#### Reading Past Month KPIs
df_past = pd.read_csv('KIWA_past_month_kpis_monthly.csv')
dim = len(df_past)
## Actual Production, Past Month
KPIs['Actual_Prod_last_month'] = df_past['MUR_Past_Month_Prod']
## Ration of Production, Actual & Projected
KPIs['Ratio_Prod_ActualvsProject'] = df_past['MUR_Past_Month_Prod'] / KPIs['Projected_Production_last month'] 
## Actual Irradiation, Past Month
KPIs['Actual_Irrad_last_month'] = df_past['MUR_Past_Month_Irrad']
## Ration of Irradiation, Actual & Projected
KPIs['Ratio_Irrad_ActualvsProject'] = df_past['MUR_Past_Month_Irrad'] / KPIs['Projected_Irradiance_last month'] 


### Saving Results
KPIs.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaDataExport/MUR_kpis.csv', index=False, float_format='%.3f')