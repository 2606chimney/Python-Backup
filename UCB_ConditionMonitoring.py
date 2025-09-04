#!/usr/bin/env python


# UCB Daily Condition Monitoring
# Ten-day rolling window with 10-minute data average interval
# For Previous Day Query

# Published on 08/12/2024


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


#### SQL Query Configuration
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='UCB_Monitoring_daily')
dim = len(df_list)

cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Abbrev_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor() 


#### SQL Querry: 10-minute average
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
# For 1-day
Date_1 = datetime.strptime(str(start_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
Date_2 = datetime.strptime(str(end_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# Remove Exceeding Date
df1 = dfx[dfx['TimeStamp'] >= str(Date_1)]
df2 = df1[df1['TimeStamp'] <= str(Date_2)]
df = df2.copy()
del df1, df2, df_list, dfx

## Saving Daily Data
df.reset_index(drop=True, inplace=True)
df.to_csv('UCB_1_Days_Query.csv', index=False)

## Appending 1-day data to an Existing File
df.to_csv('UCB_10Days_Query.csv', mode='a', index=False, header=False)
del df

## Trim the Date Length, 10-Day
dfx = pd.read_csv('UCB_10Days_Query.csv')

## Remove Duplicated Rows
df = dfx.drop_duplicates(subset=['TimeStamp'], keep=False)
del dfx


## Remove Date Older Than 10 Days
df1 = df.copy()
df1['DateTime'] = pd.to_datetime(df['TimeStamp'])
df1['Day'] = df1['DateTime'].dt.day
df1.head()
day_num = df1['Day'].unique()
num_day = len(day_num)

# Remove Date Older Than 10 Days
if num_day > 10:
    day_gone = df1['Day'].iloc[0]
    df = df1[df1['Day'] != day_gone]
else:
    df = df1.copy()
    
df = df.drop(columns = ['DateTime', 'Day'], axis=1)
df.to_csv('UCB_10Days_Query.csv', index=False)
del df, df1


#### 10-Day Data Processing
## Outlier Detection
df = pd.read_csv('UCB_10Days_Query.csv')

## Outlier Ranges
for i in range(198):
    j = i + 1
    column_name = df.columns[j]
    
    q1 = df.iloc[:,j].quantile(0.25)
    q3 = df.iloc[:,j].quantile(0.75)
    Max = df.iloc[:,j].max()
    Min = df.iloc[:,j].min()
    IQR = q3-q1
    LB = q1 - (1.5 * IQR)
    UB = q3 + (1.5 * IQR)
    
    new_tagname1 = column_name + '_LB'
    new_tagname2 = column_name + '_UB'
    
    df1 = df.copy()
    df1[new_tagname1] = LB
    df1[new_tagname2] = UB
    
    df = df1.copy()

## Calendar
df1['DateTime'] = pd.to_datetime(df['TimeStamp'])
df1['Date'] = df1['DateTime'].dt.date
df1['Year'] = df1['DateTime'].dt.year
df1['Month'] = df1['DateTime'].dt.month
df1['Day'] = df1['DateTime'].dt.day
df1['Hour'] = df1['DateTime'].dt.hour
df1.drop(labels=['TimeStamp'], axis=1, inplace=True)
column_to_move = df1.pop('DateTime')
df1.insert(0,'DateTime', column_to_move)
del column_to_move


#### Saving Daily Processed Data
df1.to_csv('UCB_10Days_Processed.csv', index=False)
df1.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/UCB_10Days_Processed.csv', index=False, float_format='%.2f')

