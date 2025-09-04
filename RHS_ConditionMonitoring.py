#!/usr/bin/env python

## Rolling Hills Single-Day Query
## Deployed on 5/22/2025


#### Importing Python Packages
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
time_change_end = timedelta(hours=5)

# Start Time
start_datetime = start_t - time_change_start

# End Time
end_datetime = end_t + time_change_end


#### 1-day Date Range Correction
Date_1 = datetime.strptime(str(start_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
Date_2 = datetime.strptime(str(end_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')


#### SQL Query Configuration
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='RHS_Monitoring_daily')
dim = len(df_list)

cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Abbrev_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor() 


#### SQL Querry: 1-hour average
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


#### Internal Calculation
## Total 'Gas Flow Fraction' Calculation
dfx['CT1_Total Gas Flw Fractn'] = dfx[['CT1_Pilot Flw Fractn','CT1_STG A Flw Fractn','CT1_STG B Flw Fractn','CT1_STG C Flw Fractn','CT1_STG D Flw Fractn']].sum(axis=1)
dfx['CT2_Total Gas Flw Fractn'] = dfx[['CT2_Pilot Flw Fractn','CT2_STG A Flw Fractn','CT2_STG B Flw Fractn','CT2_STG C Flw Fractn']].sum(axis=1)
dfx['CT3_Total Gas Flw Fractn'] = dfx[['CT3_Pilot Flw Fractn','CT3_STG A Flw Fractn','CT3_STG B Flw Fractn','CT3_STG C Flw Fractn','CT3_STG D Flw Fractn']].sum(axis=1)
dfx['CT4_Total Gas Flw Fractn'] = dfx[['CT4_Pilot Flw Fractn','CT4_STG A Flw Fractn','CT4_STG B Flw Fractn','CT4_STG C Flw Fractn']].sum(axis=1)
dfx['CT5_Total Gas Flw Fractn'] = dfx[['CT5_Pilot Flw Fractn','CT5_STG A Flw Fractn','CT5_STG B Flw Fractn','CT5_STG C Flw Fractn']].sum(axis=1)

## Calculating Mean values
# Turbine Exhaust Temp
dfx['CT1_Turb_Exh_Temp_mean'] = dfx[['CT1_Turb Exh Temp_1','CT1_Turb Exh Temp_2','CT1_Turb Exh Temp_3','CT1_Turb Exh Temp_4',
                                     'CT1_Turb Exh Temp_5','CT1_Turb Exh Temp_6','CT1_Turb Exh Temp_7','CT1_Turb Exh Temp_8',
                                     'CT1_Turb Exh Temp_9','CT1_Turb Exh Temp_10','CT1_Turb Exh Temp_11','CT1_Turb Exh Temp_12',
                                     'CT1_Turb Exh Temp_13','CT1_Turb Exh Temp_14','CT1_Turb Exh Temp_15','CT1_Turb Exh Temp_16']].mean(axis=1)

dfx['CT2_Turb_Exh_Temp_mean'] = dfx[['CT2_Turb Exh Temp_1','CT2_Turb Exh Temp_2','CT2_Turb Exh Temp_3','CT2_Turb Exh Temp_4',
                                     'CT2_Turb Exh Temp_5','CT2_Turb Exh Temp_6','CT2_Turb Exh Temp_7','CT2_Turb Exh Temp_8',
                                     'CT2_Turb Exh Temp_9','CT2_Turb Exh Temp_10','CT2_Turb Exh Temp_11','CT2_Turb Exh Temp_12',
                                     'CT2_Turb Exh Temp_13','CT2_Turb Exh Temp_14','CT2_Turb Exh Temp_15','CT2_Turb Exh Temp_16']].mean(axis=1)

dfx['CT3_Turb_Exh_Temp_mean'] = dfx[['CT3_Turb Exh Temp_1','CT3_Turb Exh Temp_2','CT3_Turb Exh Temp_3','CT3_Turb Exh Temp_4',
                                     'CT3_Turb Exh Temp_5','CT3_Turb Exh Temp_6','CT3_Turb Exh Temp_7','CT3_Turb Exh Temp_8',
                                     'CT3_Turb Exh Temp_9','CT3_Turb Exh Temp_10','CT3_Turb Exh Temp_11','CT3_Turb Exh Temp_12',
                                     'CT3_Turb Exh Temp_13','CT3_Turb Exh Temp_14','CT3_Turb Exh Temp_15','CT3_Turb Exh Temp_16']].mean(axis=1)

dfx['CT4_Turb_Exh_Temp_mean'] = dfx[['CT4_Turb Exh Temp_1','CT4_Turb Exh Temp_2','CT4_Turb Exh Temp_3','CT4_Turb Exh Temp_4',
                                     'CT4_Turb Exh Temp_5','CT4_Turb Exh Temp_6','CT4_Turb Exh Temp_7','CT4_Turb Exh Temp_8',
                                     'CT4_Turb Exh Temp_9','CT4_Turb Exh Temp_10','CT4_Turb Exh Temp_11','CT4_Turb Exh Temp_12',
                                     'CT4_Turb Exh Temp_13','CT4_Turb Exh Temp_14','CT4_Turb Exh Temp_15','CT4_Turb Exh Temp_16']].mean(axis=1)

dfx['CT5_Turb_Exh_Temp_mean'] = dfx[['CT5_Turb Exh Temp_1','CT5_Turb Exh Temp_2','CT5_Turb Exh Temp_3','CT5_Turb Exh Temp_4',
                                     'CT5_Turb Exh Temp_5','CT5_Turb Exh Temp_6','CT5_Turb Exh Temp_7','CT5_Turb Exh Temp_8',
                                     'CT5_Turb Exh Temp_9','CT5_Turb Exh Temp_10','CT5_Turb Exh Temp_11','CT5_Turb Exh Temp_12',
                                     'CT5_Turb Exh Temp_13','CT5_Turb Exh Temp_14','CT5_Turb Exh Temp_15','CT5_Turb Exh Temp_16']].mean(axis=1)

# Turbine Inlet Temp                                
dfx['CT1_Inlet_Air_Temp_U_mean'] = dfx[['CT1_Inlet Air Temp UL','CT1_Inlet Air Temp UM','CT1_Inlet Air Temp UR']].mean(axis=1)
dfx['CT1_Inlet_Air_Temp_C_mean'] = dfx[['CT1_Inlet Air Temp CL','CT1_Inlet Air Temp CM','CT1_Inlet Air Temp CR']].mean(axis=1)
dfx['CT1_Inlet_Air_Temp_L_mean'] = dfx[['CT1_Inlet Air Temp LL','CT1_Inlet Air Temp LM','CT1_Inlet Air Temp LR']].mean(axis=1)

dfx['CT2_Inlet_Air_Temp_U_mean'] = dfx[['CT2_Inlet Air Temp UL','CT2_Inlet Air Temp UM','CT2_Inlet Air Temp UR']].mean(axis=1)
dfx['CT2_Inlet_Air_Temp_C_mean'] = dfx[['CT2_Inlet Air Temp CL','CT2_Inlet Air Temp CM','CT2_Inlet Air Temp CR']].mean(axis=1)
dfx['CT2_Inlet_Air_Temp_L_mean'] = dfx[['CT2_Inlet Air Temp LL','CT2_Inlet Air Temp LM','CT2_Inlet Air Temp LR']].mean(axis=1)

dfx['CT3_Inlet_Air_Temp_U_mean'] = dfx[['CT3_Inlet Air Temp UL','CT3_Inlet Air Temp UM','CT3_Inlet Air Temp UR']].mean(axis=1)
dfx['CT3_Inlet_Air_Temp_C_mean'] = dfx[['CT3_Inlet Air Temp CL','CT3_Inlet Air Temp CM','CT3_Inlet Air Temp CR']].mean(axis=1)
dfx['CT3_Inlet_Air_Temp_L_mean'] = dfx[['CT3_Inlet Air Temp LL','CT3_Inlet Air Temp LM','CT3_Inlet Air Temp LR']].mean(axis=1)

dfx['CT4_Inlet_Air_Temp_U_mean'] = dfx[['CT4_Inlet Air Temp UL','CT4_Inlet Air Temp UM','CT4_Inlet Air Temp UR']].mean(axis=1)
dfx['CT4_Inlet_Air_Temp_C_mean'] = dfx[['CT4_Inlet Air Temp CL','CT4_Inlet Air Temp CM','CT4_Inlet Air Temp CR']].mean(axis=1)
dfx['CT4_Inlet_Air_Temp_L_mean'] = dfx[['CT4_Inlet Air Temp LL','CT4_Inlet Air Temp LM','CT4_Inlet Air Temp LR']].mean(axis=1)

dfx['CT5_Inlet_Air_Temp_U_mean'] = dfx[['CT5_Inlet Air Temp UL','CT5_Inlet Air Temp UM','CT5_Inlet Air Temp UR']].mean(axis=1)
dfx['CT5_Inlet_Air_Temp_C_mean'] = dfx[['CT5_Inlet Air Temp CL','CT5_Inlet Air Temp CM','CT5_Inlet Air Temp CR']].mean(axis=1)
dfx['CT5_Inlet_Air_Temp_L_mean'] = dfx[['CT5_Inlet Air Temp LL','CT5_Inlet Air Temp LM','CT5_Inlet Air Temp LR']].mean(axis=1)


# Combustor Flashback Temp
dfx['CT1_Combustor_FB_Temp_A_mean'] = dfx[['CT1_Combustor_1_FB_Temp A','CT1_Combustor_2_FB_Temp A','CT1_Combustor_3_FB_Temp A',
                                           'CT1_Combustor_4_FB_Temp A','CT1_Combustor_5_FB_Temp A','CT1_Combustor_6_FB_Temp A',
                                           'CT1_Combustor_7_FB_Temp A','CT1_Combustor_8_FB_Temp A','CT1_Combustor_9_FB_Temp A',
                                           'CT1_Combustor_10_FB_Temp A','CT1_Combustor_11_FB_Temp A','CT1_Combustor_12_FB_Temp A',
                                           'CT1_Combustor_13_FB_Temp A','CT1_Combustor_14_FB_Temp A','CT1_Combustor_15_FB_Temp A',
                                           'CT1_Combustor_16_FB_Temp A']].mean(axis=1)
                                     
dfx['CT2_Combustor_FB_Temp_A_mean'] = dfx[['CT2_Combustor_1_FB_Temp A','CT2_Combustor_2_FB_Temp A','CT2_Combustor_3_FB_Temp A',
                                           'CT2_Combustor_4_FB_Temp A','CT2_Combustor_5_FB_Temp A','CT2_Combustor_6_FB_Temp A',
                                           'CT2_Combustor_7_FB_Temp A','CT2_Combustor_8_FB_Temp A','CT2_Combustor_9_FB_Temp A',
                                           'CT2_Combustor_10_FB_Temp A','CT2_Combustor_11_FB_Temp A','CT2_Combustor_12_FB_Temp A',
                                           'CT2_Combustor_13_FB_Temp A','CT2_Combustor_14_FB_Temp A','CT2_Combustor_15_FB_Temp A',
                                           'CT2_Combustor_16_FB_Temp A']].mean(axis=1)
                                     
dfx['CT3_Combustor_FB_Temp_A_mean'] = dfx[['CT3_Combustor_1_FB_Temp A','CT3_Combustor_2_FB_Temp A','CT3_Combustor_3_FB_Temp A',
                                           'CT3_Combustor_4_FB_Temp A','CT3_Combustor_5_FB_Temp A','CT3_Combustor_6_FB_Temp A',
                                           'CT3_Combustor_7_FB_Temp A','CT3_Combustor_8_FB_Temp A','CT3_Combustor_9_FB_Temp A',
                                           'CT3_Combustor_10_FB_Temp A','CT3_Combustor_11_FB_Temp A','CT3_Combustor_12_FB_Temp A',
                                           'CT3_Combustor_13_FB_Temp A','CT3_Combustor_14_FB_Temp A','CT3_Combustor_15_FB_Temp A',
                                           'CT3_Combustor_16_FB_Temp A']].mean(axis=1)
                                     
dfx['CT4_Combustor_FB_Temp_A_mean'] = dfx[['CT4_Combustor_1_FB_Temp A','CT4_Combustor_2_FB_Temp A','CT4_Combustor_3_FB_Temp A',
                                           'CT4_Combustor_4_FB_Temp A','CT4_Combustor_5_FB_Temp A','CT4_Combustor_6_FB_Temp A',
                                           'CT4_Combustor_7_FB_Temp A','CT4_Combustor_8_FB_Temp A','CT4_Combustor_9_FB_Temp A',
                                           'CT4_Combustor_10_FB_Temp A','CT4_Combustor_11_FB_Temp A','CT4_Combustor_12_FB_Temp A',
                                           'CT4_Combustor_13_FB_Temp A','CT4_Combustor_14_FB_Temp A','CT4_Combustor_15_FB_Temp A',
                                           'CT4_Combustor_16_FB_Temp A']].mean(axis=1)
                                     
dfx['CT5_Combustor_FB_Temp_A_mean'] = dfx[['CT5_Combustor_1_FB_Temp A','CT5_Combustor_2_FB_Temp A','CT5_Combustor_3_FB_Temp A',
                                           'CT5_Combustor_4_FB_Temp A','CT5_Combustor_5_FB_Temp A','CT5_Combustor_6_FB_Temp A',
                                           'CT5_Combustor_7_FB_Temp A','CT5_Combustor_8_FB_Temp A','CT5_Combustor_9_FB_Temp A',
                                           'CT5_Combustor_10_FB_Temp A','CT5_Combustor_11_FB_Temp A','CT5_Combustor_12_FB_Temp A',
                                           'CT5_Combustor_13_FB_Temp A','CT5_Combustor_14_FB_Temp A','CT5_Combustor_15_FB_Temp A',
                                           'CT5_Combustor_16_FB_Temp A']].mean(axis=1)
                                     
dfx['CT1_Combustor_FB_Temp_B_mean'] = dfx[['CT1_Combustor_1_FB_Temp B','CT1_Combustor_2_FB_Temp B','CT1_Combustor_3_FB_Temp B',
                                           'CT1_Combustor_4_FB_Temp B','CT1_Combustor_5_FB_Temp B','CT1_Combustor_6_FB_Temp B',
                                           'CT1_Combustor_7_FB_Temp B','CT1_Combustor_8_FB_Temp B','CT1_Combustor_9_FB_Temp B',
                                           'CT1_Combustor_10_FB_Temp B','CT1_Combustor_11_FB_Temp B','CT1_Combustor_12_FB_Temp B',
                                           'CT1_Combustor_13_FB_Temp B','CT1_Combustor_14_FB_Temp B','CT1_Combustor_15_FB_Temp B',
                                           'CT1_Combustor_16_FB_Temp B']].mean(axis=1)
                                     
dfx['CT2_Combustor_FB_Temp_B_mean'] = dfx[['CT2_Combustor_1_FB_Temp B','CT2_Combustor_2_FB_Temp B','CT2_Combustor_3_FB_Temp B',
                                           'CT2_Combustor_4_FB_Temp B','CT2_Combustor_5_FB_Temp B','CT2_Combustor_6_FB_Temp B',
                                           'CT2_Combustor_7_FB_Temp B','CT2_Combustor_8_FB_Temp B','CT2_Combustor_9_FB_Temp B',
                                           'CT2_Combustor_10_FB_Temp B','CT2_Combustor_11_FB_Temp B','CT2_Combustor_12_FB_Temp B',
                                           'CT2_Combustor_13_FB_Temp B','CT2_Combustor_14_FB_Temp B','CT2_Combustor_15_FB_Temp B',
                                           'CT2_Combustor_16_FB_Temp B']].mean(axis=1)
                                     
dfx['CT3_Combustor_FB_Temp_B_mean'] = dfx[['CT3_Combustor_1_FB_Temp B','CT3_Combustor_2_FB_Temp B','CT3_Combustor_3_FB_Temp B',
                                           'CT3_Combustor_4_FB_Temp B','CT3_Combustor_5_FB_Temp B','CT3_Combustor_6_FB_Temp B',
                                           'CT3_Combustor_7_FB_Temp B','CT3_Combustor_8_FB_Temp B','CT3_Combustor_9_FB_Temp B',
                                           'CT3_Combustor_10_FB_Temp B','CT3_Combustor_11_FB_Temp B','CT3_Combustor_12_FB_Temp B',
                                           'CT3_Combustor_13_FB_Temp B','CT3_Combustor_14_FB_Temp B','CT3_Combustor_15_FB_Temp B',
                                           'CT3_Combustor_16_FB_Temp B']].mean(axis=1)
                                     
dfx['CT4_Combustor_FB_Temp_B_mean'] = dfx[['CT4_Combustor_1_FB_Temp B','CT4_Combustor_2_FB_Temp B','CT4_Combustor_3_FB_Temp B',
                                           'CT4_Combustor_4_FB_Temp B','CT4_Combustor_5_FB_Temp B','CT4_Combustor_6_FB_Temp B',
                                           'CT4_Combustor_7_FB_Temp B','CT4_Combustor_8_FB_Temp B','CT4_Combustor_9_FB_Temp B',
                                           'CT4_Combustor_10_FB_Temp B','CT4_Combustor_11_FB_Temp B','CT4_Combustor_12_FB_Temp B',
                                           'CT4_Combustor_13_FB_Temp B','CT4_Combustor_14_FB_Temp B','CT4_Combustor_15_FB_Temp B',
                                           'CT4_Combustor_16_FB_Temp B']].mean(axis=1)
                                     
dfx['CT5_Combustor_FB_Temp_B_mean'] = dfx[['CT5_Combustor_1_FB_Temp B','CT5_Combustor_2_FB_Temp B','CT5_Combustor_3_FB_Temp B',
                                           'CT5_Combustor_4_FB_Temp B','CT5_Combustor_5_FB_Temp B','CT5_Combustor_6_FB_Temp B',
                                           'CT5_Combustor_7_FB_Temp B','CT5_Combustor_8_FB_Temp B','CT5_Combustor_9_FB_Temp B',
                                           'CT5_Combustor_10_FB_Temp B','CT5_Combustor_11_FB_Temp B','CT5_Combustor_12_FB_Temp B',
                                           'CT5_Combustor_13_FB_Temp B','CT5_Combustor_14_FB_Temp B','CT5_Combustor_15_FB_Temp B',
                                           'CT5_Combustor_16_FB_Temp B']].mean(axis=1)
                                     
# Blade Path Temp
dfx['CT1_Blade_Path_Temp_1_mean'] = dfx[['CT1_Blade Path Temp 1_1','CT1_Blade Path Temp 2_1','CT1_Blade Path Temp 3_1',
                                        'CT1_Blade Path Temp 4_1','CT1_Blade Path Temp 5_1','CT1_Blade Path Temp 6_1',
                                        'CT1_Blade Path Temp 7_1','CT1_Blade Path Temp 8_1','CT1_Blade Path Temp 9_1',
                                        'CT1_Blade Path Temp 10_1','CT1_Blade Path Temp 11_1','CT1_Blade Path Temp 12_1',
                                        'CT1_Blade Path Temp 13_1','CT1_Blade Path Temp 14_1','CT1_Blade Path Temp 15_1',
                                        'CT1_Blade Path Temp 16_1']].mean(axis=1)
                                     
dfx['CT2_Blade_Path_Temp_1_mean'] = dfx[['CT2_Blade Path Temp 1_1','CT2_Blade Path Temp 2_1','CT2_Blade Path Temp 3_1',
                                        'CT2_Blade Path Temp 4_1','CT2_Blade Path Temp 5_1','CT2_Blade Path Temp 6_1',
                                        'CT2_Blade Path Temp 7_1','CT2_Blade Path Temp 8_1','CT2_Blade Path Temp 9_1',
                                        'CT2_Blade Path Temp 10_1','CT2_Blade Path Temp 11_1','CT2_Blade Path Temp 12_1',
                                        'CT2_Blade Path Temp 13_1','CT2_Blade Path Temp 14_1','CT2_Blade Path Temp 15_1',
                                        'CT2_Blade Path Temp 16_1']].mean(axis=1)
                                     
dfx['CT3_Blade_Path_Temp_1_mean'] = dfx[['CT3_Blade Path Temp 1_1','CT3_Blade Path Temp 2_1','CT3_Blade Path Temp 3_1',
                                        'CT3_Blade Path Temp 4_1','CT3_Blade Path Temp 5_1','CT3_Blade Path Temp 6_1',
                                        'CT3_Blade Path Temp 7_1','CT3_Blade Path Temp 8_1','CT3_Blade Path Temp 9_1',
                                        'CT3_Blade Path Temp 10_1','CT3_Blade Path Temp 11_1','CT3_Blade Path Temp 12_1',
                                        'CT3_Blade Path Temp 13_1','CT3_Blade Path Temp 14_1','CT3_Blade Path Temp 15_1',
                                        'CT3_Blade Path Temp 16_1']].mean(axis=1)
                                     
dfx['CT4_Blade_Path_Temp_1_mean'] = dfx[['CT4_Blade Path Temp 1_1','CT4_Blade Path Temp 2_1','CT4_Blade Path Temp 3_1',
                                        'CT4_Blade Path Temp 4_1','CT4_Blade Path Temp 5_1','CT4_Blade Path Temp 6_1',
                                        'CT4_Blade Path Temp 7_1','CT4_Blade Path Temp 8_1','CT4_Blade Path Temp 9_1',
                                        'CT4_Blade Path Temp 10_1','CT4_Blade Path Temp 11_1','CT4_Blade Path Temp 12_1',
                                        'CT4_Blade Path Temp 13_1','CT4_Blade Path Temp 14_1','CT4_Blade Path Temp 15_1',
                                        'CT4_Blade Path Temp 16_1']].mean(axis=1)
                                     
dfx['CT5_Blade_Path_Temp_1_mean'] = dfx[['CT5_Blade Path Temp 1_1','CT5_Blade Path Temp 2_1','CT5_Blade Path Temp 3_1',
                                        'CT5_Blade Path Temp 4_1','CT5_Blade Path Temp 5_1','CT5_Blade Path Temp 6_1',
                                        'CT5_Blade Path Temp 7_1','CT5_Blade Path Temp 8_1','CT5_Blade Path Temp 9_1',
                                        'CT5_Blade Path Temp 10_1','CT5_Blade Path Temp 11_1','CT5_Blade Path Temp 12_1',
                                        'CT5_Blade Path Temp 13_1','CT5_Blade Path Temp 14_1','CT5_Blade Path Temp 15_1',
                                        'CT5_Blade Path Temp 16_1']].mean(axis=1) 
                                     
dfx['CT1_Blade_Path_Temp_2_mean'] = dfx[['CT1_Blade Path Temp 1_2','CT1_Blade Path Temp 2_2','CT1_Blade Path Temp 3_2',
                                        'CT1_Blade Path Temp 4_2','CT1_Blade Path Temp 5_2','CT1_Blade Path Temp 6_2',
                                        'CT1_Blade Path Temp 7_2','CT1_Blade Path Temp 8_2','CT1_Blade Path Temp 9_2',
                                        'CT1_Blade Path Temp 10_2','CT1_Blade Path Temp 11_2','CT1_Blade Path Temp 12_2',
                                        'CT1_Blade Path Temp 13_2','CT1_Blade Path Temp 14_2','CT1_Blade Path Temp 15_2',
                                        'CT1_Blade Path Temp 16_2']].mean(axis=1)
                                     
dfx['CT2_Blade_Path_Temp_2_mean'] = dfx[['CT2_Blade Path Temp 1_2','CT2_Blade Path Temp 2_2','CT2_Blade Path Temp 3_2',
                                        'CT2_Blade Path Temp 4_2','CT2_Blade Path Temp 5_2','CT2_Blade Path Temp 6_2',
                                        'CT2_Blade Path Temp 7_2','CT2_Blade Path Temp 8_2','CT2_Blade Path Temp 9_2',
                                        'CT2_Blade Path Temp 10_2','CT2_Blade Path Temp 11_2','CT2_Blade Path Temp 12_2',
                                        'CT2_Blade Path Temp 13_2','CT2_Blade Path Temp 14_2','CT2_Blade Path Temp 15_2',
                                        'CT2_Blade Path Temp 16_2']].mean(axis=1)
                                     
dfx['CT3_Blade_Path_Temp_2_mean'] = dfx[['CT3_Blade Path Temp 1_2','CT3_Blade Path Temp 2_2','CT3_Blade Path Temp 3_2',
                                        'CT3_Blade Path Temp 4_2','CT3_Blade Path Temp 5_2','CT3_Blade Path Temp 6_2',
                                        'CT3_Blade Path Temp 7_2','CT3_Blade Path Temp 8_2','CT3_Blade Path Temp 9_2',
                                        'CT3_Blade Path Temp 10_2','CT3_Blade Path Temp 11_2','CT3_Blade Path Temp 12_2',
                                        'CT3_Blade Path Temp 13_2','CT3_Blade Path Temp 14_2','CT3_Blade Path Temp 15_2',
                                        'CT3_Blade Path Temp 16_2']].mean(axis=1)
                                     
dfx['CT4_Blade_Path_Temp_2_mean'] = dfx[['CT4_Blade Path Temp 1_2','CT4_Blade Path Temp 2_2','CT4_Blade Path Temp 3_2',
                                        'CT4_Blade Path Temp 4_2','CT4_Blade Path Temp 5_2','CT4_Blade Path Temp 6_2',
                                        'CT4_Blade Path Temp 7_2','CT4_Blade Path Temp 8_2','CT4_Blade Path Temp 9_2',
                                        'CT4_Blade Path Temp 10_2','CT4_Blade Path Temp 11_2','CT4_Blade Path Temp 12_2',
                                        'CT4_Blade Path Temp 13_2','CT4_Blade Path Temp 14_2','CT4_Blade Path Temp 15_2',
                                        'CT4_Blade Path Temp 16_2']].mean(axis=1)
                                     
dfx['CT5_Blade_Path_Temp_2_mean'] = dfx[['CT5_Blade Path Temp 1_2','CT5_Blade Path Temp 2_2','CT5_Blade Path Temp 3_2',
                                        'CT5_Blade Path Temp 4_2','CT5_Blade Path Temp 5_2','CT5_Blade Path Temp 6_2',
                                        'CT5_Blade Path Temp 7_2','CT5_Blade Path Temp 8_2','CT5_Blade Path Temp 9_2',
                                        'CT5_Blade Path Temp 10_2','CT5_Blade Path Temp 11_2','CT5_Blade Path Temp 12_2',
                                        'CT5_Blade Path Temp 13_2','CT5_Blade Path Temp 14_2','CT5_Blade Path Temp 15_2',
                                        'CT5_Blade Path Temp 16_2']].mean(axis=1)
                                     
# Generator Stator Winding Temp
dfx['CT1_Gen_Ph_A_Winding_Temp_mean'] = dfx[['CT1_Gen Ph_A Stator Winding Temp A','CT1_Gen Ph_A Stator Winding Temp B']].mean(axis=1)
dfx['CT2_Gen_Ph_A_Winding_Temp_mean'] = dfx[['CT2_Gen Ph_A Stator Winding Temp A','CT2_Gen Ph_A Stator Winding Temp B']].mean(axis=1)
dfx['CT3_Gen_Ph_A_Winding_Temp_mean'] = dfx[['CT3_Gen Ph_A Stator Winding Temp A','CT3_Gen Ph_A Stator Winding Temp B']].mean(axis=1)
dfx['CT4_Gen_Ph_A_Winding_Temp_mean'] = dfx[['CT4_Gen Ph_A Stator Winding Temp A','CT4_Gen Ph_A Stator Winding Temp B']].mean(axis=1)
dfx['CT5_Gen_Ph_A_Winding_Temp_mean'] = dfx[['CT5_Gen Ph_A Stator Winding Temp A','CT5_Gen Ph_A Stator Winding Temp B']].mean(axis=1)
                                     
dfx['CT1_Gen_Ph_B_Winding_Temp_mean'] = dfx[['CT1_Gen Ph_B Stator Winding Temp A','CT1_Gen Ph_B Stator Winding Temp B']].mean(axis=1)
dfx['CT2_Gen_Ph_B_Winding_Temp_mean'] = dfx[['CT2_Gen Ph_B Stator Winding Temp A','CT2_Gen Ph_B Stator Winding Temp B']].mean(axis=1)
dfx['CT3_Gen_Ph_B_Winding_Temp_mean'] = dfx[['CT3_Gen Ph_B Stator Winding Temp A','CT3_Gen Ph_B Stator Winding Temp B']].mean(axis=1)
dfx['CT4_Gen_Ph_B_Winding_Temp_mean'] = dfx[['CT4_Gen Ph_B Stator Winding Temp A','CT4_Gen Ph_B Stator Winding Temp B']].mean(axis=1)
dfx['CT5_Gen_Ph_B_Winding_Temp_mean'] = dfx[['CT5_Gen Ph_B Stator Winding Temp A','CT5_Gen Ph_B Stator Winding Temp B']].mean(axis=1)
                                     
dfx['CT1_Gen_Ph_C_Winding_Temp_mean'] = dfx[['CT1_Gen Ph_C Stator Winding Temp A','CT1_Gen Ph_C Stator Winding Temp B']].mean(axis=1)
dfx['CT2_Gen_Ph_C_Winding_Temp_mean'] = dfx[['CT2_Gen Ph_C Stator Winding Temp A','CT2_Gen Ph_C Stator Winding Temp B']].mean(axis=1)
dfx['CT3_Gen_Ph_C_Winding_Temp_mean'] = dfx[['CT3_Gen Ph_C Stator Winding Temp A','CT3_Gen Ph_C Stator Winding Temp B']].mean(axis=1)
dfx['CT4_Gen_Ph_C_Winding_Temp_mean'] = dfx[['CT4_Gen Ph_C Stator Winding Temp A','CT4_Gen Ph_C Stator Winding Temp B']].mean(axis=1)
dfx['CT5_Gen_Ph_C_Winding_Temp_mean'] = dfx[['CT5_Gen Ph_C Stator Winding Temp A','CT5_Gen Ph_C Stator Winding Temp B']].mean(axis=1)

# Generator Inlet/Outlet Air Temp
dfx['CT1_Gen_Inlet_Air_Temp_mean'] = dfx[['CT1_Gen Inlet CE Cold Air Temp','CT1_Gen Inlet TE Cold Air Temp']].mean(axis=1)
dfx['CT2_Gen_Inlet_Air_Temp_mean'] = dfx[['CT2_Gen Inlet CE Cold Air Temp','CT2_Gen Inlet TE Cold Air Temp']].mean(axis=1)
dfx['CT3_Gen_Inlet_Air_Temp_mean'] = dfx[['CT3_Gen Inlet CE Cold Air Temp','CT3_Gen Inlet TE Cold Air Temp']].mean(axis=1)
dfx['CT4_Gen_Inlet_Air_Temp_mean'] = dfx[['CT4_Gen Inlet CE Cold Air Temp','CT4_Gen Inlet TE Cold Air Temp']].mean(axis=1)
dfx['CT5_Gen_Inlet_Air_Temp_mean'] = dfx[['CT5_Gen Inlet CE Cold Air Temp','CT5_Gen Inlet TE Cold Air Temp']].mean(axis=1)
                                     
dfx['CT1_Gen_Outlet_Air_Temp_mean'] = dfx[['CT1_Gen CE Warm Air Temp','CT1_Gen TE Warm Air Temp']].mean(axis=1)
dfx['CT2_Gen_Outlet_Air_Temp_mean'] = dfx[['CT2_Gen CE Warm Air Temp','CT2_Gen TE Warm Air Temp']].mean(axis=1)
dfx['CT3_Gen_Outlet_Air_Temp_mean'] = dfx[['CT3_Gen CE Warm Air Temp','CT3_Gen TE Warm Air Temp']].mean(axis=1)
dfx['CT4_Gen_Outlet_Air_Temp_mean'] = dfx[['CT4_Gen CE Warm Air Temp','CT4_Gen TE Warm Air Temp']].mean(axis=1)
dfx['CT5_Gen_Outlet_Air_Temp_mean'] = dfx[['CT5_Gen CE Warm Air Temp','CT5_Gen TE Warm Air Temp']].mean(axis=1)

# Generator Bearing Temp
dfx['CT1_Gen_TE_Brg_Temp_mean'] = dfx[['CT1_Gen TE Brg Temp A','CT1_Gen TE Brg Temp B','CT1_Gen TE Brg Temp C','CT1_Gen TE Brg Temp D']].mean(axis=1)
dfx['CT2_Gen_TE_Brg_Temp_mean'] = dfx[['CT2_Gen TE Brg Temp A','CT2_Gen TE Brg Temp B','CT2_Gen TE Brg Temp C','CT2_Gen TE Brg Temp D']].mean(axis=1)
dfx['CT3_Gen_TE_Brg_Temp_mean'] = dfx[['CT3_Gen TE Brg Temp A','CT3_Gen TE Brg Temp B','CT3_Gen TE Brg Temp C','CT3_Gen TE Brg Temp D']].mean(axis=1)
dfx['CT4_Gen_TE_Brg_Temp_mean'] = dfx[['CT4_Gen TE Brg Temp A','CT4_Gen TE Brg Temp B','CT4_Gen TE Brg Temp C','CT4_Gen TE Brg Temp D']].mean(axis=1)
dfx['CT5_Gen_TE_Brg_Temp_mean'] = dfx[['CT5_Gen TE Brg Temp A','CT5_Gen TE Brg Temp B','CT5_Gen TE Brg Temp C','CT5_Gen TE Brg Temp D']].mean(axis=1)

dfx['CT1_Gen_CE_Brg_Temp_mean'] = dfx[['CT1_Gen CE Brg Temp A','CT1_Gen CE Brg Temp B','CT1_Gen CE Brg Temp C','CT1_Gen CE Brg Temp D']].mean(axis=1)
dfx['CT2_Gen_CE_Brg_Temp_mean'] = dfx[['CT2_Gen CE Brg Temp A','CT2_Gen CE Brg Temp B','CT2_Gen CE Brg Temp C','CT2_Gen CE Brg Temp D']].mean(axis=1)
dfx['CT3_Gen_CE_Brg_Temp_mean'] = dfx[['CT3_Gen CE Brg Temp A','CT3_Gen CE Brg Temp B','CT3_Gen CE Brg Temp C','CT3_Gen CE Brg Temp D']].mean(axis=1)
dfx['CT4_Gen_CE_Brg_Temp_mean'] = dfx[['CT4_Gen CE Brg Temp A','CT4_Gen CE Brg Temp B','CT4_Gen CE Brg Temp C','CT4_Gen CE Brg Temp D']].mean(axis=1)
dfx['CT5_Gen_CE_Brg_Temp_mean'] = dfx[['CT5_Gen CE Brg Temp A','CT5_Gen CE Brg Temp B','CT5_Gen CE Brg Temp C','CT5_Gen CE Brg Temp D']].mean(axis=1)


#### Remove Exceeding Date
df1 = dfx[dfx['TimeStamp'] >= str(Date_1)]
df2 = df1[df1['TimeStamp'] <= str(Date_2)]

# Replacing NaN with Zeros
df = df2.fillna(0)
del df1, df2, df_list, dfx

# Calendar
df['DateTime'] = pd.to_datetime(df['TimeStamp'])
df['Date'] = df['DateTime'].dt.date
df['Year'] = df['DateTime'].dt.year
df['Month'] = df['DateTime'].dt.month
df['Day'] = df['DateTime'].dt.day
df.drop(labels=['DateTime'], axis=1, inplace=True)


#### Appending 1-day data to an Existing File
df.reset_index(drop=True, inplace=True)
df.to_csv('RHS_Ten_Days_Data.csv', mode='a', index=False, header=False)
del df


#### Removing Date Older Than 10 Days
df = pd.read_csv('RHS_Ten_Days_Data.csv')

# Count number of days from current data
df1 = df.copy()
day_num = df1['Day'].unique()
num_day = len(day_num)

# Remove Date Older Than 10 Days
if num_day > 10:
    day_gone = df1['Day'].iloc[0]
    df = df1[df1['Day'] != day_gone]


#### Saving a Final Results    
df.to_csv('RHS_Ten_Days_Data.csv', index=False, float_format='%.2f')
df.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/RHS_Ten_Days_Data.csv', index=False, float_format='%.2f')
del df, df1