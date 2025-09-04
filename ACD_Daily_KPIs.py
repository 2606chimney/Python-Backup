#!/usr/bin/env python
# coding: utf-8

# KIWA Power Daily KPI Calculations
# Daily KPI calculations- 11/16/2022
# Modified to reflect the stuck values
# Modified for prevention from that the dataframe is highly fragmented
# Formula for Weather Corrected PR is modified (IVT-3 & -4): 12/09/2022


##### Import Python Packages
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

from numpy import mean
from pandas import DataFrame
from datetime import date, datetime, timedelta
from matplotlib import pyplot


#### Reading Tag List
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='ACD_daily')
dim = len(df_list)

#### SQL Configuration
cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Abbrev_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor() 

#### Date Configuration
today = datetime.today()
Today = pd.to_datetime('today').normalize()
Yesterday = Today - timedelta(days = 1)
start_t = Yesterday
# end_t = Today - timedelta(minutes=10)
end_t = Today
# Changing UTC to EST time
time_change_start = timedelta(hours=1)
time_change_end = timedelta(hours=6)
# Start Time
start_datetime = start_t - time_change_start
# End Time
end_datetime = end_t + time_change_end


#### SQL Querry
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


#### Saving Daily Data
df.to_csv('ACD_Daily_KPI_Query.csv', index=False)


#### Processing
## Data Cleaning
# NaN Removal
df = df.fillna(0)

## Stuck Value Removal
# Correcting KWAC Values
IVT_1_KWAC_mod = df['IVT-1_KWAC'] * df['IVT-1_Available']
df['IVT-1_KWAC'] = IVT_1_KWAC_mod
del IVT_1_KWAC_mod

IVT_2_KWAC_mod = df['IVT-2_KWAC'] * df['IVT-2_Available']
df['IVT-2_KWAC'] = IVT_2_KWAC_mod
del IVT_2_KWAC_mod

IVT_3_KWAC_mod = df['IVT-3_KWAC'] * df['IVT-3_Available']
df['IVT-3_KWAC'] = IVT_3_KWAC_mod
del IVT_3_KWAC_mod

IVT_4_KWAC_mod = df['IVT-4_KWAC'] * df['IVT-4_Available']
df['IVT-4_KWAC'] = IVT_4_KWAC_mod
del IVT_4_KWAC_mod

IVT_5_KWAC_mod = df['IVT-5_KWAC'] * df['IVT-5_Available']
df['IVT-5_KWAC'] = IVT_5_KWAC_mod
del IVT_5_KWAC_mod

# Correcting KWDC Values
IVT_1_KWDC_mod = df['IVT-1_KWDC'] * df['IVT-1_Available']
df['IVT-1_KWDC'] = IVT_1_KWDC_mod
del IVT_1_KWDC_mod

IVT_2_KWDC_mod = df['IVT-2_KWDC'] * df['IVT-2_Available']
df['IVT-2_KWDC'] = IVT_2_KWDC_mod
del IVT_2_KWDC_mod

IVT_3_KWDC_mod = df['IVT-3_KWDC'] * df['IVT-3_Available']
df['IVT-3_KWDC'] = IVT_3_KWDC_mod
del IVT_3_KWDC_mod

IVT_4_KWDC_mod = df['IVT-4_KWDC'] * df['IVT-4_Available']
df['IVT-4_KWDC'] = IVT_4_KWDC_mod
del IVT_4_KWDC_mod

IVT_5_KWDC_mod = df['IVT-5_KWDC'] * df['IVT-5_Available']
df['IVT-5_KWDC'] = IVT_5_KWDC_mod
del IVT_5_KWDC_mod

## Correcting String Values
ivt = ['IVT-1', 'IVT-2', 'IVT-3', 'IVT-4', 'IVT-5']
amps = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20']
volts = ['01','02','03','04','05','06','07','08','09','10']
str1 = 'StringAmps'
str2 = 'StringVolt'

# Fixing AmpString Values
for i in range(0, 5):
    ivt_str = ivt[i] + '_'
    avail_str = ivt_str + 'Available'
    
    for j in range(0, len(amps)):
        amp_str = ivt_str + str1 + amps[j]
        amp_val_mod = df[amp_str] * df[avail_str]
        df[amp_str] = amp_val_mod
        del amp_val_mod
        
# Fixing VoltString Values
for i in range(0, 5):
    ivt_str = ivt[i] + '_'
    avail_str = ivt_str + 'Available'

    for j in range(0, len(volts)):    
        volt_str = ivt_str + str2 + volts[j]
        volt_val_mod = df[volt_str] * df[avail_str]
        df[volt_str] = volt_val_mod
        del volt_val_mod
 
## Calendar       
df1 = df.copy()
df1['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

df1['date'] = df1['TimeStamp'].dt.date
df1['year'] = df1['TimeStamp'].dt.year
df1['quarter'] = df1['TimeStamp'].dt.quarter
df1['month'] = df1['TimeStamp'].dt.month
df1['week'] = df1['TimeStamp'].dt.isocalendar().week
df1['day'] = df1['TimeStamp'].dt.day
df1['hour'] = df1['TimeStamp'].dt.hour
del df
df = df1.copy()

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

## Converting Data Types
# Listing 'object' columns 
obj_cols = []
for i in df.columns:
    if df[i].dtype == 'object':
        obj_cols.append(i)

# Converting 'object' data type to 'float' data type
for i in range(len(obj_cols)):
    if i != 0:
        df[obj_cols[i]] = pd.to_numeric(df[obj_cols[i]], errors='coerce')
        

#### Time Based Availability (TBA)
df1 = df[df['MET-1_Irrad_Totalizer'] > 0]

ivt_1_down = df1[df1['IVT-1_KWAC'] <= 0.1]
ivt_2_down = df1[df1['IVT-2_KWAC'] <= 0.1]
ivt_3_down = df1[df1['IVT-3_KWAC'] <= 0.1]
ivt_4_down = df1[df1['IVT-4_KWAC'] <= 0.1]
ivt_5_down = df1[df1['IVT-5_KWAC'] <= 0.1]

TBA_ivt_1 = (1-(len(ivt_1_down)/len(df1['MET-1_Irrad_Totalizer']))) * 100
TBA_ivt_2 = (1-(len(ivt_2_down)/len(df1['MET-1_Irrad_Totalizer']))) * 100
TBA_ivt_3 = (1-(len(ivt_3_down)/len(df1['MET-1_Irrad_Totalizer']))) * 100
TBA_ivt_4 = (1-(len(ivt_4_down)/len(df1['MET-1_Irrad_Totalizer']))) * 100
TBA_ivt_5 = (1-(len(ivt_5_down)/len(df1['MET-1_Irrad_Totalizer']))) * 100

TBA_ivt = [TBA_ivt_1, TBA_ivt_2, TBA_ivt_3, TBA_ivt_4, TBA_ivt_5]
TBA_plant = sum(TBA_ivt)/len(TBA_ivt)
del df1

current_date = str(df['Date'].values[0])
result = {'Date': current_date, 'TBA_Plant': [TBA_plant], 'TBA_IVT_1': [TBA_ivt_1], 'TBA_IVT_2': [TBA_ivt_2], 'TBA_IVT_3': [TBA_ivt_3], 
          'TBA_IVT_4': [TBA_ivt_4], 'TBA_IVT_5': [TBA_ivt_5]}

KPIs = pd.DataFrame(result, columns = ['Date', 'TBA_Plant', 'TBA_IVT_1', 'TBA_IVT_2', 'TBA_IVT_3', 'TBA_IVT_4', 'TBA_IVT_5'])
KPIs = KPIs.round(2)


#### Actual Performance Ratio (PR)
# Some Constants
Gstc = 1000 # irradiance at standard test conditions
module = [306, 252, 238, 252, 252]
delta_temp_coeff = -0.35 # temperature coefficient for power(%/C, megative in sign) that corresponds to the installed modules
Tcell_typ_avg_1 = 65.97 # average cell temperature computed from one year of weather data using the project weather file
Tcell_typ_avg_2 = 61.68
Tcell_typ_avg = (Tcell_typ_avg_1 + Tcell_typ_avg_2) / 2

numer_IVT_1 = df['IVT-1_KWAC'].sum()
numer_IVT_2 = df['IVT-2_KWAC'].sum()
numer_IVT_3 = df['IVT-3_KWAC'].sum()
numer_IVT_4 = df['IVT-4_KWAC'].sum()
numer_IVT_5 = df['IVT-5_KWAC'].sum()

module_pk_pwr = 445
Pstc_IVT_1 = module[0] * module_pk_pwr
Pstc_IVT_2 = module[1] * module_pk_pwr
Pstc_IVT_3 = module[2] * module_pk_pwr
Pstc_IVT_4 = module[3] * module_pk_pwr
Pstc_IVT_5 = module[4] * module_pk_pwr

tot_irrad_1 = df['MET-1_Irrad_Totalizer'].sum()
#tot_irrad_2 = df['MET-2_Irrad_Totalizer'].sum()
tot_irrad_2 = df['MET-1_Irrad_Totalizer'].sum()

Gpoa_1 = tot_irrad_1/1000
Gpoa_2 = tot_irrad_2/1000
Gpoa_avg = (Gpoa_1 + Gpoa_2) / 2

denom_IVT_1 = Pstc_IVT_1 * (Gpoa_1 / Gstc)
denom_IVT_2 = Pstc_IVT_2 * (Gpoa_1 / Gstc)
denom_IVT_3 = Pstc_IVT_3 * (Gpoa_avg / Gstc)
denom_IVT_4 = Pstc_IVT_4 * (Gpoa_avg / Gstc)
denom_IVT_5 = Pstc_IVT_5 * (Gpoa_1 / Gstc)

PR_ivt_1 = (numer_IVT_1 / denom_IVT_1) * 100
PR_ivt_2 = (numer_IVT_2 / denom_IVT_2) * 100
PR_ivt_3 = (numer_IVT_3 / denom_IVT_3) * 100
PR_ivt_4 = (numer_IVT_4 / denom_IVT_4) * 100
PR_ivt_5 = (numer_IVT_5 / denom_IVT_5) * 100
PR_plant = sum([PR_ivt_1, PR_ivt_2, PR_ivt_3, PR_ivt_4, PR_ivt_5])/len([PR_ivt_1, PR_ivt_2, PR_ivt_3, PR_ivt_4, PR_ivt_5])

result = {'Date': current_date, 'PR_Plant': [PR_plant], 'PR_IVT_1': [PR_ivt_1], 'PR_IVT_2': [PR_ivt_2], 'PR_IVT_3': [PR_ivt_3], 
          'PR_IVT_4': [PR_ivt_4], 'PR_IVT_5': [PR_ivt_5]}

PR = pd.DataFrame(result, columns = ['Date', 'PR_Plant', 'PR_IVT_1', 'PR_IVT_2', 'PR_IVT_3', 'PR_IVT_4', 'PR_IVT_5'])
KPIs = pd.merge(KPIs, PR, how='outer', on=['Date','Date'])


#### Weather Corrected Performance Ratio (PR)
Tcell_1_max = df['MET-1_PanelTemp'].max()
#Tcell_2_max = df['MET-2_PanelTemp'].max()
Tcell_2_max = df['MET-1_PanelTemp'].max()
Tcell_avg_max = (Tcell_1_max + Tcell_2_max) / 2

denom_IVT_1_adj = (Pstc_IVT_1 * (Gpoa_1 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))
denom_IVT_2_adj = (Pstc_IVT_2 * (Gpoa_1 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))
denom_IVT_3_adj = (Pstc_IVT_3 * (Gpoa_avg / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))
denom_IVT_4_adj = (Pstc_IVT_4 * (Gpoa_avg / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))
denom_IVT_5_adj = (Pstc_IVT_5 * (Gpoa_1 / Gstc)) * (1 - (delta_temp_coeff/100) * (Tcell_typ_avg_1 - Tcell_1_max))

PR_ivt_1_adj = (numer_IVT_1 / denom_IVT_1_adj) * 100
PR_ivt_2_adj = (numer_IVT_2 / denom_IVT_2_adj) * 100
PR_ivt_3_adj = (numer_IVT_3 / denom_IVT_3_adj) * 100
PR_ivt_4_adj = (numer_IVT_4 / denom_IVT_4_adj) * 100
PR_ivt_5_adj = (numer_IVT_5 / denom_IVT_5_adj) * 100
PR_plant_adj = sum([PR_ivt_1_adj, PR_ivt_2_adj, PR_ivt_3_adj, PR_ivt_4_adj, PR_ivt_5_adj])/len([PR_ivt_1_adj, PR_ivt_2_adj, PR_ivt_3_adj, PR_ivt_4_adj, PR_ivt_5_adj])


#### Merging and Dataframe
result = {'Date': current_date, 'PR_Plant_Weather': [PR_plant_adj], 'PR_IVT_1_Weather': [PR_ivt_1_adj], 
          'PR_IVT_2_Weather': [PR_ivt_2_adj], 'PR_IVT_3_Weather': [PR_ivt_3_adj], 
          'PR_IVT_4_Weather': [PR_ivt_4_adj], 'PR_IVT_5_Weather': [PR_ivt_5_adj]}

PR_Weather = pd.DataFrame(result, columns = ['Date', 'PR_Plant_Weather', 'PR_IVT_1_Weather', 'PR_IVT_2_Weather',
                                     'PR_IVT_3_Weather', 'PR_IVT_4_Weather', 'PR_IVT_5_Weather'])
KPIs = pd.merge(KPIs, PR_Weather, how='outer', on=['Date','Date'])


#### Soiling Loss
# PDCstc = [ivt-1 (445*18), ivt-2 (445*18), ivt-3 (445*17), ivt-4 (445*18), ivt-5 (445*18)]
PDCstc = [8010, 8010, 7565, 8010, 8010]

ivt = ['IVT-1', 'IVT-2', 'IVT-3', 'IVT-4', 'IVT-5']
amps = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20']
volts = ['01','01','02','02','03','03','04','04','05','05','06','06','07','07','08','08','09','09','10','10']
string_1 = 'StringAmps'
string_2 = 'StringVolt'

dfx = pd.DataFrame()
for i in range(0,5):
    ivt_str = ivt[i] + '_'
    
    for j in range(0,20):
        amp_str = ivt_str + string_1 + amps[j]
        volt_str = ivt_str + string_2 + volts[j]
        dcpower = df[amp_str] * df[volt_str]
        col_name = 'dcpower_' + ivt_str + string_1[0:6] + amps[j]
        dfx[col_name] = dcpower
        
dcpower_numeric = dfx.sum()
tot_irrad_avg = (tot_irrad_1 + tot_irrad_2) / 2

for i in range(0,5):
    s = i + 1
    new_str = 'PR_' + ivt[i][0:3] + '_' + str(s)
    
    PR_string = []
    for j in range(0,20):
        new_col_name = new_str + '_' + string_1[0:6] + amps[j]
        if s == 1:
            new_col_name = dcpower_numeric[j] / (PDCstc[i] * (tot_irrad_1 / Gstc))
            PR_string.append("{:.2f}".format(new_col_name))
            
        elif s == 2:
            new_col_name = dcpower_numeric[20+j] / (PDCstc[i] * (tot_irrad_1 / Gstc))
            PR_string.append("{:.2f}".format(new_col_name))
            
        elif s == 3:
            new_col_name = dcpower_numeric[40+j] / (PDCstc[i] * (tot_irrad_avg / Gstc))
            PR_string.append("{:.2f}".format(new_col_name))
            
        elif s == 4:
            new_col_name = dcpower_numeric[60+j] / (PDCstc[i] * (tot_irrad_avg / Gstc))
            PR_string.append("{:.2f}".format(new_col_name))
            
        elif s == 5:
            new_col_name = dcpower_numeric[80+j] / (PDCstc[i] * (tot_irrad_1 / Gstc))
            PR_string.append("{:.2f}".format(new_col_name))
    
    if i == 0:
        PR_IVT_STR = pd.DataFrame(PR_string)
        PR_IVT_STR.columns = [new_str]
    else:
        PR_IVT_STR[new_str] = PR_string
        
PR_IVT_STR = PR_IVT_STR.astype(float)

## Soiling Loss, String-Level
for i in range(0,5):
    s = i + 1
    ivt_num = 'PR_IVT_' + str(i+1)
    
    Soiling_string = []
    for j in range(0,20):
        column_name = 'IVT_' + str(i+1) + '_Soiling_Loss'
        soiling_1 = 100 - ((PR_IVT_STR[ivt_num][j] * 100)/0.9) # Formula-1
        #soiling_2 = (PR_IVT_STR[ivt_num][j] / (PR_IVT_STR[ivt_num].sum())) * 100 # Formula-2
        Soiling_string.append("{:.2f}".format(soiling_1))
    
    if i == 0:
        Soiling_IVT = pd.DataFrame(Soiling_string)
        Soiling_IVT.columns = [column_name]
    else:
        Soiling_IVT[column_name] = Soiling_string
        
Soiling_IVT = Soiling_IVT.astype(float)

## Soiling Loss, Inverter Level
for k in range(0,5):
    ivt_id = k+1
    String = Soiling_IVT.columns[k]
    
    if ivt_id == 1:
        KPIs['Soiling_Loss_IVT_1'] = (Soiling_IVT[String].iloc[0] + Soiling_IVT[String].iloc[1] + 
        Soiling_IVT[String].iloc[2] + Soiling_IVT[String].iloc[3] + Soiling_IVT[String].iloc[4] + 
        Soiling_IVT[String].iloc[5] + Soiling_IVT[String].iloc[6] + Soiling_IVT[String].iloc[7] +
        Soiling_IVT[String].iloc[8] + Soiling_IVT[String].iloc[9] + Soiling_IVT[String].iloc[10] + 
        Soiling_IVT[String].iloc[11] + Soiling_IVT[String].iloc[12] + Soiling_IVT[String].iloc[13] + 
        Soiling_IVT[String].iloc[14] + Soiling_IVT[String].iloc[16] + Soiling_IVT[String].iloc[18]) / 17
    
    elif ivt_id == 2:
        KPIs['Soiling_Loss_IVT_2'] = (Soiling_IVT[String].iloc[0] + Soiling_IVT[String].iloc[1] + 
        Soiling_IVT[String].iloc[2] + Soiling_IVT[String].iloc[3] + Soiling_IVT[String].iloc[4] + 
        Soiling_IVT[String].iloc[5] + Soiling_IVT[String].iloc[6] + Soiling_IVT[String].iloc[7] +
        Soiling_IVT[String].iloc[8] + Soiling_IVT[String].iloc[9] + Soiling_IVT[String].iloc[10] + 
        Soiling_IVT[String].iloc[12] + Soiling_IVT[String].iloc[14] + Soiling_IVT[String].iloc[16]) / 14
        
    elif ivt_id == 3:
        KPIs['Soiling_Loss_IVT_3'] = (Soiling_IVT[String].iloc[0] + Soiling_IVT[String].iloc[2] + Soiling_IVT[String].iloc[4] + 
        Soiling_IVT[String].iloc[5] + Soiling_IVT[String].iloc[6] + Soiling_IVT[String].iloc[7] + Soiling_IVT[String].iloc[8] + 
        Soiling_IVT[String].iloc[10] + Soiling_IVT[String].iloc[11] + Soiling_IVT[String].iloc[12] + 
        Soiling_IVT[String].iloc[13] + Soiling_IVT[String].iloc[14] + Soiling_IVT[String].iloc[15] + 
        Soiling_IVT[String].iloc[17]) / 14
        
    elif ivt_id == 4:
        KPIs['Soiling_Loss_IVT_4'] = (Soiling_IVT[String].iloc[0] + Soiling_IVT[String].iloc[1] + 
        Soiling_IVT[String].iloc[2] + Soiling_IVT[String].iloc[3] + Soiling_IVT[String].iloc[4] + 
        Soiling_IVT[String].iloc[5] + Soiling_IVT[String].iloc[6] + Soiling_IVT[String].iloc[7] +
        Soiling_IVT[String].iloc[8] + Soiling_IVT[String].iloc[9] + Soiling_IVT[String].iloc[11] + 
        Soiling_IVT[String].iloc[12] + Soiling_IVT[String].iloc[14] + Soiling_IVT[String].iloc[16]) / 14
        
    elif ivt_id == 5:
        KPIs['Soiling_Loss_IVT_5'] = (Soiling_IVT[String].iloc[0] + Soiling_IVT[String].iloc[1] + 
        Soiling_IVT[String].iloc[2] + Soiling_IVT[String].iloc[3] + Soiling_IVT[String].iloc[4] + 
        Soiling_IVT[String].iloc[5] + Soiling_IVT[String].iloc[6] + Soiling_IVT[String].iloc[7] +
        Soiling_IVT[String].iloc[8] + Soiling_IVT[String].iloc[9] + Soiling_IVT[String].iloc[10] + 
        Soiling_IVT[String].iloc[12] + Soiling_IVT[String].iloc[14] + Soiling_IVT[String].iloc[16]) / 14
    
KPIs['Soiling_Loss_Plant'] = (KPIs['Soiling_Loss_IVT_1'] + KPIs['Soiling_Loss_IVT_2'] + KPIs['Soiling_Loss_IVT_3'] + 
                              KPIs['Soiling_Loss_IVT_4'] + KPIs['Soiling_Loss_IVT_5']) / 5


#### Importing Projection Irradiance and Production
# Finding Current Month
Current_Month = df['Month'].iloc[-1]

# Read Projected Production & Irradiance Number from a Reference File
Projected_Production = [67.36, 70.17, 84.34, 81.34, 81.62, 71.68, 74.74, 72.97, 65.14, 67.7, 66.59, 63.41]
Projected_Irradiance = [133.7, 143.2, 180.2, 178.8, 179.6, 157.0, 163.4, 158.9, 135.9, 139.5, 133.0, 123.9]
KPIs['Projected_Production_last month'] = Projected_Production[Current_Month-2] # Last month's value
KPIs['Projected_Irradiance_last month'] = Projected_Irradiance[Current_Month-2] # Last month's value


#### Reading Past Month KPIs
df_past = pd.read_csv('KIWA_past_month_kpis_monthly.csv')
# Actual Production, Past Month
KPIs['Actual_Prod_last_month'] = df_past['ACD_Past_Month_Prod']
## Ration of Production, Actual & Projected
KPIs['Ratio_Prod_ActualvsProject'] = df_past['ACD_Past_Month_Prod'] / KPIs['Projected_Production_last month'] 

# Actual Irradiation, Past Month
KPIs['Actual_Irrad_last_month'] = df_past['ACD_Past_Month_Irrad']
## Ration of Irradiation, Actual & Projected
KPIs['Ratio_Irrad_ActualvsProject'] = df_past['ACD_Past_Month_Irrad'] / KPIs['Projected_Irradiance_last month'] 


#### Saving Results
KPIs.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaDataExport/ACD_kpis.csv', index=False, float_format='%.2f')
#### Copy a File a from One Folder to Another
#shutil.copyfile('C:/ProgramData/Anaconda3/ACD_kpis.csv', 'C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaDataExport/ACD_kpis.csv')
#============== END ===============#
