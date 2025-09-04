#!/usr/bin/env python
# coding: utf-8

## Shiloh-IV Daily Temperature Condition Monitoring- 09/01/2022
## Modified- 11/16/2022


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
from scipy import stats
from numpy import *
from pandas import DataFrame
from datetime import date, datetime, timedelta


#### Date Configuration
today = datetime.today()
today = pd.to_datetime('today').normalize()
# 10-day Span
startDate = today - timedelta(days = 10)
endDate = today - timedelta(days = 0)  
# Changing UTC to EST time
time_change_start = timedelta(hours=1)
time_change_end = timedelta(hours=6)
# Start Time
start_datetime = startDate - time_change_start
# End Time
end_datetime = endDate + time_change_end


#### Import Tag List
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='SHL_daily_Corrected')
dim = len(df_list)


#### SQL Configuration
cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Abbrev_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor()


#### Data Query
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

## Converting UTC time scale to US EST time scale
dfx['TimeStamp'] = pd.to_datetime(dfx['TimeStamp'], errors='coerce')
dfx['TimeStamp_EST'] = dfx['TimeStamp'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S')
EST_Time = dfx['TimeStamp_EST']
dfx.drop(labels=['TimeStamp'], axis=1, inplace=True)
dfx.insert(0,'TimeStamp', EST_Time)
dfx.drop(labels=['TimeStamp_EST'], axis=1, inplace=True)

## Date Correction
Date_1 = datetime.strptime(str(startDate), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
Date_2 = datetime.strptime(str(endDate), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

## Remove Exceeding Date
df1 = dfx[dfx['TimeStamp'] >= str(Date_1)]
df2 = df1[df1['TimeStamp'] <= str(Date_2)]
df = df2.copy()
del df1, df2, df_list, dfx


#### Saving Weekly Data
df.to_csv('SHL_Daily_Temp_Query_Corrected.csv', index=False, float_format='%.2f')
del df


#### Load Data
df = pd.read_csv('SHL_Daily_Temp_Query_Corrected.csv')


#### Preprocessing
## Changing Data Structure by Turbine ID
df_date = df.iloc[0:, 0:1] # date_time
date_list = df_date.columns.values.tolist()

## Signal Live Finder
df = df.fillna(0)

# 'Active Power' Column Location: [10, 21, 32, 43, 54, 65,.....]
col_list = list(df.columns.values)

df1 = df.copy()
for i in range(0, 50):
    # Extract Only Active Power Column
    j = 10 * (i + 1) + i
    k = j + 1

    col_name = col_list[j]
    tur_id = col_name[0:4]
    
    new_col_name = tur_id + 'live'
    df1[new_col_name] = df[col_name].apply(lambda x: 1 if x > 0  else 0)
    
## Stuck Value Correction
col_list = list(df1.columns.values)

k = 0
m = 0
for i in range(0, 50): # Turbine ID
    df2 = df.copy()
    
    # Extract Temp Columns Under A Turbine
    for j in range(0, 9): # Temp Columns
        k = j + 1 + m
        
        col_name = col_list[k]
        
        new_col_name = col_name + '_' + 'corrected'
        
        tur_id = col_name[0:4]
        tur_live = tur_id + 'live'
        
        new_val = df1[col_name] * df1[tur_live]

        df2[col_name] = new_val
        
        df2 = df2.copy().rename(columns={col_name: new_col_name})
        df = df2.copy()
     
    m = k + 2
del df1

## Remove Duplicate Columns
df = df.loc[:, ~df.columns.duplicated()].copy()

## Corrected Signal (Stuck Value) Version
# Number of Bearings (= 9): [Gn_Brg-1, Gn_Brg-2, Sft_Brg-1, Sft_Brg-2, Gn_Stator_Brg, Hub, Rtr_Brg, Gbx_Oil, Cnvt_Air_Sply] 
# Column Location: [1, 2, 3, 4, 5, 6, 7, 8, 9]
df_Tol = df.iloc[0:, 0:1]
col_loc = [1, 2, 3, 4, 5, 6, 7, 8, 9]
num_jump = 11

for k in range(0, len(col_loc)):
    idx_end = 0
    
    df_temp = pd.DataFrame()
    for i in range(1, 51):
        
        idx_start = num_jump * (i - 1) + col_loc[k]
        idx_end = idx_start + 1
        
        x = df.iloc[0:, idx_start:idx_end]
        
        if i == 1:
            df_temp = x
        else:
            df_temp = df_temp.join(x)

    Upper_Mean = []
    Lower_Mean = []
    for j in range(0, len(df_temp)):
        X = df_temp.iloc[j]
        X = np.where(np.isnan(X), 0, X)

        # IQR
        Q1 = np.percentile(X, 25, interpolation = 'midpoint')
        Q3 = np.percentile(X, 75, interpolation = 'midpoint')
        IQR = Q3 - Q1
    
        # Upper & lower bound
        upper = Q3 + 1.5 * IQR
        lower = Q1 - 1.5 * IQR
    
        # Removing the Outliers
        X = X[X <= upper]
        X = X[X >= lower]
    
        # Mean Calculation
        Mean_temp = X.mean()
    
        # Upper Maen
        UMean = Mean_temp * 1.3
        Upper_Mean.append(UMean)
        # Lower Mean
        LMean = Mean_temp * 0.7
        Lower_Mean.append(LMean)

    Upper_Mean = pd.Series(Upper_Mean)
    Lower_Mean = pd.Series(Lower_Mean)

    # Creating New Tag Names
    c_name = df_temp.columns[0]
    tagname = c_name[4:]
    new_tagname1 = 'UMean_' + tagname
    new_tagname2 = 'LMean_' + tagname

    # Adding into a DataFrame
    df_Tol[new_tagname1] = Upper_Mean
    df_Tol[new_tagname2] = Lower_Mean
    del df_temp, x, X, Upper_Mean, Lower_Mean
outlier_tag_list = df_Tol.iloc[0:, 1:].columns.values.tolist()

## Extracting Turbine ID
col_loc = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
num_Col = len(col_loc)

End = 1
df_concat =[]
for i in range(1, 51):
    
    # Chopping
    Start = End
    End = Start + len(col_loc)
    df_tag = df.iloc[0:, Start:End]
    
    # turbine id extraction
    col_list = df_tag.columns.values.tolist()
    
    col_name = col_list[0]
    
    unit_ID = col_name[0:3]
    
    df_tag['Turbine_ID'] = pd.Series([unit_ID for x in range(len(df_tag.index))])

    # Move 'Turbine_ID' column into front column
    turbine = df_tag['Turbine_ID']
    df_tag.drop(labels=['Turbine_ID'], axis=1, inplace=True)
    df_tag.insert(0,'Turbine_ID', turbine)

    # replacing with short tag names
    new_col_name = []
    col_list = df_tag.columns.values.tolist()
    
    for j in range(0, len(col_list)):
        if j < 1:
            column_name = col_list[j]
            replace_col_name = column_name
            new_col_name.append(replace_col_name)
            
        else:
            column_name = col_list[j]
            replace_col_name = column_name[4:]
            new_col_name.append(replace_col_name)    
    
    # replacing with new column names
    df_tag.columns = [new_col_name]

    # merging date part and tag part
    df_unit = df_date.join(df_tag)
    
    df_unit = pd.merge(df_unit, df_Tol, on='TimeStamp')

    # making all column lists
    all_lists = date_list + new_col_name + outlier_tag_list
    
    df_unit.columns = [all_lists]

    if i == 1:
        df_concat = df_unit
        frames = df_concat.copy()
    else:
        # appending all turbine data
        df_concat = pd.concat([frames, df_unit])
        frames = df_concat.copy()

# Fix for multi Index issue
df_concat.columns = df_concat.columns.map('_'.join)

del df
df = df_concat.copy()
del df_concat, df_Tol, df_unit, df_tag, all_lists, frames

#### Calendar Info
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
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

Date = df['day']
df.drop(labels=['day'], axis=1, inplace=True)
df.insert(1,'Day', Date)

Date = df['week']
df.drop(labels=['week'], axis=1, inplace=True)
df.insert(1,'Week', Date)

Date = df['month']
df.drop(labels=['month'], axis=1, inplace=True)
df.insert(1,'Month', Date)

Date = df['quarter']
df.drop(labels=['quarter'], axis=1, inplace=True)
df.insert(1,'Quarter', Date)

Date = df['year']
df.drop(labels=['year'], axis=1, inplace=True)
df.insert(1,'Year', Date)

Date = df['date']
df.drop(labels=['date'], axis=1, inplace=True)
df.insert(1,'Date', Date)


#### CAN BE DELETED
# Listing 'object' columns 
obj_cols = []
for i in df.columns:
    if df[i].dtype == 'object':
        obj_cols.append(i)
obj_cols


#### CAN BE DELETED
# Converting 'object' data type to 'float' data type
# for i in range(len(obj_cols)):
#     if i != 0:
#         df[obj_cols[i]] = pd.to_numeric(df[obj_cols[i]], errors='coerce')


#### Risk Score
## Gen Brg-1
Z = df['UMean_GnBrg1_corrected'] - df['GnBrg1_corrected']
# RiskScore
def Score_Gen_Brg1(x):
    if x > 10.0:
        Score_Gen_Brg1 = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_Gen_Brg1 = 30.0
    elif x <= 2.0 and x > 0:
        Score_Gen_Brg1 = 50.0
    elif x == 0.0:
        Score_Gen_Brg1 = 75.0
    else:
        Score_Gen_Brg1 = 90.0
    return Score_Gen_Brg1
df.loc[:,'Score_Gen_Brg1_corrected'] = Z.apply(lambda x: Score_Gen_Brg1(x))


## Gen Brg-2
Z = df['UMean_GnBrg2_corrected'] - df['GnBrg2_corrected']
# RiskScore
def Score_Gen_Brg2(x):
    if x > 10.0:
        Score_Gen_Brg2 = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_Gen_Brg2 = 30.0
    elif x <= 2.0 and x > 0:
        Score_Gen_Brg2 = 50.0
    elif x == 0.0:
        Score_Gen_Brg2 = 75.0
    else:
        Score_Gen_Brg2 = 90.0
    return Score_Gen_Brg2
df.loc[:,'Score_Gen_Brg2_corrected'] = Z.apply(lambda x: Score_Gen_Brg2(x))


## ShfBrg1
Z = df['UMean_TrmTmpShfBrg1_corrected'] - df['TrmTmpShfBrg1_corrected']
# RiskScore
def Score_TrmTmpShfBrg1(x):
    if x > 10.0:
        Score_TrmTmpShfBrg1 = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_TrmTmpShfBrg1 = 30.0
    elif x <= 2.0 and x > 0:
        Score_TrmTmpShfBrg1 = 50.0
    elif x == 0.0:
        Score_TrmTmpShfBrg1 = 75.0
    else:
        Score_TrmTmpShfBrg1 = 90.0
    return Score_TrmTmpShfBrg1
df.loc[:,'Score_TrmTmpShfBrg1_corrected'] = Z.apply(lambda x: Score_TrmTmpShfBrg1(x))


## ShfBrg2
Z = df['UMean_TrmTmpShfBrg2_corrected'] - df['TrmTmpShfBrg2_corrected']
# RiskScore
def Score_TrmTmpShfBrg2(x):
    if x > 10.0:
        Score_TrmTmpShfBrg2 = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_TrmTmpShfBrg2 = 30.0
    elif x <= 2.0 and x > 0:
        Score_TrmTmpShfBrg2 = 50.0
    elif x == 0.0:
        Score_TrmTmpShfBrg2 = 75.0
    else:
        Score_TrmTmpShfBrg2 = 90.0
    return Score_TrmTmpShfBrg2
df.loc[:,'Score_TrmTmpShfBrg2_corrected'] = Z.apply(lambda x: Score_TrmTmpShfBrg2(x))


## GnTmpSta
Z = df['UMean_GnTmpSta_corrected'] - df['GnTmpSta_corrected']
# RiskScore
def Score_GnTmpSta(x):
    if x > 10.0:
        Score_GnTmpSta = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_GnTmpSta = 30.0
    elif x <= 2.0 and x > 0:
        Score_GnTmpSta = 50.0
    elif x == 0.0:
        Score_GnTmpSta = 75.0
    else:
        Score_GnTmpSta = 90.0
    return Score_GnTmpSta
df.loc[:,'Score_GnTmpSta_corrected'] = Z.apply(lambda x: Score_GnTmpSta(x))


## HubTmp
Z = df['UMean_HubTmp_corrected'] - df['HubTmp_corrected']
# RiskScore
def Score_HubTmp(x):
    if x > 10.0:
        Score_HubTmp = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_HubTmp = 30.0
    elif x <= 2.0 and x > 0:
        Score_HubTmp = 50.0
    elif x == 0.0:
        Score_HubTmp = 75.0
    else:
        Score_HubTmp = 90.0
    return Score_HubTmp
df.loc[:,'Score_HubTmp_corrected'] = Z.apply(lambda x: Score_HubTmp(x))


## RotBrgTmp
Z = df['UMean_RotBrgTmp_corrected'] - df['RotBrgTmp_corrected']
# RiskScore
def Score_RotBrgTmp(x):
    if x > 10.0:
        Score_RotBrgTmp = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_RotBrgTmp = 30.0
    elif x <= 2.0 and x > 0:
        Score_RotBrgTmp = 50.0
    elif x == 0.0:
        Score_RotBrgTmp = 75.0
    else:
        Score_RotBrgTmp = 90.0
    return Score_RotBrgTmp
df.loc[:,'Score_RotBrgTmp_corrected'] = Z.apply(lambda x: Score_RotBrgTmp(x))


## GbxOil
Z = df['UMean_TrmTmpGbxOil_corrected'] - df['TrmTmpGbxOil_corrected']
# RiskScore
def Score_TrmTmpGbxOil(x):
    if x > 10.0:
        Score_TrmTmpGbxOil = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_TrmTmpGbxOil = 30.0
    elif x <= 2.0 and x > 0:
        Score_TrmTmpGbxOil = 50.0
    elif x == 0.0:
        Score_TrmTmpGbxOil = 75.0
    else:
        Score_TrmTmpGbxOil = 90.0
    return Score_TrmTmpGbxOil
df.loc[:,'Score_TrmTmpGbxOil_corrected'] = Z.apply(lambda x: Score_TrmTmpGbxOil(x))


# CnvAirTmp
Z = df['UMean_CnvAirTmp_corrected'] - df['CnvAirTmp_corrected']
# RiskScore
def Score_CnvAirTmp(x):
    if x > 10.0:
        Score_CnvAirTmp = 0.0
    elif x <= 10.0 and x > 2.0:
        Score_CnvAirTmp = 30.0
    elif x <= 2.0 and x > 0:
        Score_CnvAirTmp = 50.0
    elif x == 0.0:
        Score_CnvAirTmp = 75.0
    else:
        Score_CnvAirTmp = 90.0  
    return Score_CnvAirTmp
df.loc[:,'Score_CnvAirTmp_corrected'] = Z.apply(lambda x: Score_CnvAirTmp(x))


#### Save to a Desination Folder
df.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/SHL_Daily_Temp_Condition_Corrected.csv', index=False, float_format='%.2f')




