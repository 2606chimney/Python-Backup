#!/usr/bin/env python


## Quisqueya KPI Processing- Daily, version-4
## QQY Daily Processing with Hourly Samples- 6/19/2023
    # 1. Daily query data processing and concatenate with 31-day processed data- 5/18/2023
    # 2. Part-1: Proccessed Data; Part-2: Vertical structure table with 3-KPIs
    # 3. New daily "Gas Energy" and "Gas Consumption" calculations- 6/19/2023
    # 4. Empty column error handling- 7/18/2023
    


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
from iapws import IAPWS97


#### Single-Day Configuration
today = datetime.today()
Today = pd.to_datetime('today').normalize()
Yesterday = Today - timedelta(days = 1)
start_t = Yesterday
end_t = Today
# Changing UTC to EST time
time_change_start = timedelta(hours=1)
time_change_end = timedelta(hours=10)
# Start Time
start_datetime = start_t - time_change_start
# End Time
end_datetime = end_t + time_change_end


#### Read Tag List
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='QQY_31days')
dim = len(df_list)


#### SQL Configuration
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


#### Date Correction
# For 1-day
Date_1 = datetime.strptime(str(end_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# Remove Exceeding Date
df = dfx[dfx['TimeStamp'] <= str(Date_1)]
del df_list, dfx


#### Replacing Row
Shape = df.shape
Row_Val = Shape[0] - 24
df1 = df.iloc[Row_Val-1:, 0:]
df1 = df1.reset_index(drop=True)

New =str(df1['TimeStamp'].iloc[1])
Old = str(df1['TimeStamp'].iloc[0])
df2 = df1.replace(Old, New)
del df1

df1 = df2.drop(labels=[1], axis=0)
df1 = df1.reset_index(drop=True)
del df2


#### Calendar 
df1['TimeStamp'] = pd.to_datetime(df1['TimeStamp'])
df1['Date'] = df1['TimeStamp'].dt.date
df1['Date'] = pd.to_datetime(df1['Date'])
df1['date'] = df1['Date'].dt.strftime('%m/%d/%Y')
df1.drop(labels=['Date'], axis=1, inplace=True)
df1['month'] = df1['TimeStamp'].dt.month
df1['day'] = df1['TimeStamp'].dt.day


#### Saving Daily Data
df1.to_csv('QQY_Daily_Query_v2.csv', index=False)
del df, df1


#### Importing Query Data
df = pd.read_csv('QQY_Daily_Query_v2.csv')


#### Daily Export MWh Calculation from Cumulative Data
col_index = [0,27,54,81,108,135,162,189,216,243,270,297]
col_List = [4,5,6,7,8,9,10,11,14,15,16,19,20]

new_engine_name = ['Engine01','Engine02','Engine03','Engine04','Engine05','Engine06','Engine07','Engine08',
                   'Engine09','Engine10','Engine11','Engine12']
new_col_name = ['_daily_export, MWh','_daily_export_Gasmode, MWh','_daily_export_Dieselmode, MWh','_daily_export_HFOmode, MWh',
               '_daily_run, Hrs','_daily_run_Gas, Hrs','_daily_run_Diesel, Hrs','_daily_run_HFO, Hrs',
               '_daily_flow_Gas, Kg','_daily_flow_HFO, Kg','_daily_flow_LFO, Kg',
               '_daily_turbine_wash, Cnt','_daily_compr_wash, Cnt']

## Engine
for i in range(len(col_index)):
    I = col_index[i]
    
    for j in range(len(col_List)):
        J = col_List[j]
        k = I + J
        X = (df.iloc[:,[k]]).dropna()
        Y = X.reset_index(drop=True)
        new_tag_name = new_engine_name[i] + new_col_name[j]
        
        #------------------- Empty Column Error Handling ------------------#
        if (Y.empty == True):
            df[new_tag_name] = 0
            col_to_move = df.pop(new_tag_name)
            df.insert(k, new_tag_name, col_to_move)
            df.drop(labels=[str(df.columns[k+1])], axis=1, inplace=True)
            
        else:
            last_val = float(Y.iloc[-1])
            first_val = float(Y.iloc[0])
            calc_val =  last_val - first_val       
            df[new_tag_name] = calc_val
            col_to_move = df.pop(new_tag_name)
            df.insert(k, new_tag_name, col_to_move)
            df.drop(labels=[str(df.columns[k+1])], axis=1, inplace=True)
        #------------------------------------------------------------------#


#### Engine Efficiency Correction Processing
col_index = [26,53,80,107,134,161,188,215,242,269,296,323]

## Engine
for i in range(len(col_index)):
    j = col_index[i]
    col_name = df.columns[j]
    new_col_name = col_name[0:9] + 'daily_corrected_efficiency (%)'

    df[new_col_name]  = df[col_name].apply(lambda x: x if x < 55.0 else 0)

    X = df[df[new_col_name] != 0]
    calc_val = X[new_col_name].mean()
    df[new_col_name] = calc_val
    
    col_to_move = df.pop(new_col_name)
    df.insert(j, new_col_name, col_to_move)
    df.drop(labels=[str(df.columns[j+1])], axis=1, inplace=True)


#### Steam Turbine -
## Daily Export, MWh Calculation
X = df.dropna(subset=['ST_Activ_E_Expt_MWh'])
Y = X.reset_index(drop=True)

last_val = Y['ST_Activ_E_Expt_MWh'].iloc[-1]
first_val = Y['ST_Activ_E_Expt_MWh'].iloc[0]
calc_val =  last_val - first_val
df['ST_daily_export, MWh'] = calc_val

# Repositioning the Colunm
to_move = df['ST_daily_export, MWh']
df.drop(labels=['ST_daily_export, MWh'], axis=1, inplace=True)
df.insert(328,'ST_daily_export, MWh', to_move)
df.drop(labels=['ST_Activ_E_Expt_MWh'], axis=1, inplace=True)

## Daily Running Hour, Hrs Calculation
X = df.dropna(subset=['ST_Run_Hour'])
Y = X.reset_index(drop=True)

last_val = Y['ST_Run_Hour'].iloc[-1]
first_val = Y['ST_Run_Hour'].iloc[0]
calc_val =  last_val - first_val
df['ST_daily_run, Hrs'] = calc_val

# Repositioning the Colunm
to_move = df['ST_daily_run, Hrs']
df.drop(labels=['ST_daily_run, Hrs'], axis=1, inplace=True)
df.insert(332,'ST_daily_run, Hrs', to_move)
df.drop(labels=['ST_Run_Hour'], axis=1, inplace=True)

## Daily Average ST Efficiency, % Calculation
X = df[df['ST_EFFICIENCY'] != 0]
calc_val = X['ST_EFFICIENCY'].mean()
df['ST_daily_efficiency, %'] = calc_val
df.drop(labels=['ST_EFFICIENCY'], axis=1, inplace=True)

## Daily Average ST Heat Rate, 
X = df[df['ST_HEAT_RATE'] != 0]
calc_val = X['ST_HEAT_RATE'].mean()
df['ST_daily_heatrate, kJ/kWh'] = calc_val
df.drop(labels=['ST_HEAT_RATE'], axis=1, inplace=True)

## Daily Average Steam Flow
X = df[df['ST_STEAM_FLOW_TURBINE'] != 0]
calc_val = X['ST_STEAM_FLOW_TURBINE'].mean()
df['ST_daily_steamflow, kg/h'] = calc_val
df.drop(labels=['ST_STEAM_FLOW_TURBINE'], axis=1, inplace=True)


#### Total Output, MWh 
df['Total Daily Export, MWh'] = (df['Engine01_daily_export, MWh'] + df['Engine02_daily_export, MWh'] +
    df['Engine03_daily_export, MWh'] + df['Engine04_daily_export, MWh'] + df['Engine05_daily_export, MWh'] +
    df['Engine06_daily_export, MWh'] + df['Engine07_daily_export, MWh'] + df['Engine08_daily_export, MWh'] +
    df['Engine09_daily_export, MWh'] + df['Engine10_daily_export, MWh'] + df['Engine11_daily_export, MWh'] + 
    df['Engine12_daily_export, MWh'] + df['ST_daily_export, MWh'])


#### Repositioning the Colunm 
to_move = df['Total Daily Export, MWh']
df.drop(labels=['Total Daily Export, MWh'], axis=1, inplace=True)
df.insert(347,'Total Daily Export, MWh', to_move)

to_move = df['ST_daily_heatrate, kJ/kWh']
df.drop(labels=['ST_daily_heatrate, kJ/kWh'], axis=1, inplace=True)
df.insert(348,'ST_daily_heatrate, kJ/kWh', to_move)

to_move = df['ST_daily_steamflow, kg/h']
df.drop(labels=['ST_daily_steamflow, kg/h'], axis=1, inplace=True)
df.insert(349,'ST_daily_steamflow, kg/h', to_move)

to_move = df['ST_daily_efficiency, %']
df.drop(labels=['ST_daily_efficiency, %'], axis=1, inplace=True)
df.insert(350,'ST_daily_efficiency, %', to_move)


#### Added on 6/19/2023: New Calculations for "Gas Energy" and "Gas Consumption"
engine_idx = ['01','02','03','04','05','06','07','08','09','10','11','12']
# Engine
for i in range(12):
    engine_num = 'Engine' + engine_idx[i]
    engine_run = engine_num + '_Engine_Running'
    gas_energy = engine_num + '_Gas_Energy'
    gas_consume = engine_num + '_Gas_Consumption'
    
    # Corrected Engine_Running
    df['Engine_Run_Corrected'] = df[engine_run].apply(lambda x: 0 if x < 1.0 else x)
    
    # Calculating "Gas Energy"
    df['Gas_Energy_Corrected'] = df['Engine_Run_Corrected'] * df[gas_energy]
    X = df[df['Gas_Energy_Corrected'] != 0]
    calc_val = X['Gas_Energy_Corrected'].mean()
    df[gas_energy] = calc_val
    
    # Calculating "Gas Consumption"
    df['Gas_Consume_Corrected'] = df['Engine_Run_Corrected'] * df[gas_consume]
    X = df[df['Gas_Consume_Corrected'] != 0]
    calc_val = X['Gas_Consume_Corrected'].mean()
    df[gas_consume] = calc_val
del X
df = df.drop(columns=['Engine_Run_Corrected', 'Gas_Energy_Corrected', 'Gas_Consume_Corrected'])


#### Appending 1-day data to an Existing Data File
df.to_csv('QQY_KPIs_Processing_daily_p1.csv', mode='a', index=False, header=False)


#### Trimming Off More Than 31 Days  
## Read Data
dfx = pd.read_csv('QQY_KPIs_Processing_daily_p1.csv')

## Remove Duplicated Rows
df = dfx.drop_duplicates(subset=['TimeStamp'], keep='last')
del dfx

## Remove Date Older Than 31 Days
day_num = df['date'].unique()
num_day = len(day_num)

if num_day > 31:
    day_to_remove = df['date'].iloc[0]
    df1 = df[df['date'] != day_to_remove]
    
else:
    df1 = df.copy()

del df

## Saving Results
df1.to_csv('QQY_KPIs_Processing_daily_p1.csv', index=False, float_format='%.2f')
df1.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/QQY_KPIs_Processing_daily_p1.csv', index=False, float_format='%.2f')
