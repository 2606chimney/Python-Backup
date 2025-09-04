#!/usr/bin/env python


## Sowega Daily Condition Monitoring- Temp & Vib
## Quisqueya Daily Query- 10-min Sampling- 7/17/2023


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
time_change_end = timedelta(hours=6)

# Start Time
start_datetime = start_t - time_change_start

# End Time
end_datetime = end_t + time_change_end


#### Read Tag List
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='SOW_Monitoring_daily')
dim = len(df_list)


#### SQL Configuration
cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Abbrev_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor() 


#### SQL Querry: 10-Minute average
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


#### Date Correction
Date_1 = datetime.strptime(str(start_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
Date_2 = datetime.strptime(str(end_t), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')

# Remove Exceeding Date
df1 = dfx[dfx['TimeStamp'] >= str(Date_1)]
df2 = df1[df1['TimeStamp'] <= str(Date_2)]
df2.reset_index(drop=True, inplace=True)
df = df2.copy()
del df1, df2, df_list, dfx


#### Calendar
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
df['Date'] = df['TimeStamp'].dt.date
df['Month'] = df['TimeStamp'].dt.month
df['Day'] = df['TimeStamp'].dt.day
df['Hour'] = df['TimeStamp'].dt.hour
df['Min'] = df['TimeStamp'].dt.minute


#### Saving Single-Day Data into a CSV File
df = df.fillna(0)
df.to_csv('SOW_1Day_Query.csv', mode='a', index=False, header=False)
del df


#### Retain 10-Day Data
## Read Saved Data File
df = pd.read_csv('SOW_1Day_Query.csv')

## Remove Duplicated Rows
dfx = df.drop_duplicates(subset=['TimeStamp'], keep='first')
del df

## Remove Date Older Than 10 Days
day_num = dfx['Day'].unique()
num_day = len(day_num)
day_gone = dfx['Day'].iloc[0]
df = dfx[dfx['Day'] != day_gone]
df.reset_index(drop=True, inplace=True)
del dfx


#### Saving a Final Results
df.to_csv('SOW_1Day_Query.csv', index=False, float_format='%.2f')
df.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/SOW_1Day_Query.csv', index=False, float_format='%.2f')