#!/usr/bin/env python

## A short version of 03_06_2023-- 04/11/2023
## Connected to Power BI "Shiloh-IV_Report_v17.pbix"


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
import os

from numpy import mean
from pandas import DataFrame
from datetime import date, datetime, timedelta


####-------------------- PART-I: SQL Query -----------------------------------
## Read Tag List
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='Shiloh_IV')
dim = len(df_list)

## One Week Configuration
# 1-Week Configuration
today = datetime.today()
today = pd.to_datetime('today').normalize()

startDate = today - timedelta(days = today.weekday() + 7)
endDate = today - timedelta(days = today.weekday() + 0)
    
# Changing UTC to EST time
time_change_start = timedelta(hours=1)
time_change_end = timedelta(hours=6)

# Start Time
start_datetime = startDate - time_change_start

# End Time
end_datetime = endDate + time_change_end

## SQL Configuration
cstring = 'DSN=ROC_DSN; Database=ODBC-SCADA'
df_tag = pd.DataFrame(df_list, columns = ['Tag_List','Short_Name'])
conn = pyodbc.connect(cstring) 
cursor = conn.cursor()

## Weekly Query
# SQL Querry
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
del frames, rows, df_date, df_val
            
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
    
## Remove Duplicate Date- Daylight Savings
dfz = df.drop_duplicates(subset=['TimeStamp'], keep='first')

## Saving Weekly Data: Append with New Data
dfz.to_csv('SHL_3weeks_Data.csv', mode='a', index=False, header=False)
del df, df_list, df1, df2, dfx, dfz, df_tag, EST_Time

## Removing First 7-Day Data
# Load Data
df = pd.read_csv('SHL_3weeks_Data.csv')
df = df.fillna(0)

# Calendar
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
df['Week'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.isocalendar().week

# Retain Only Three Weeks Data
week_num = df['Week'].unique()
excld_wk = week_num[0]
#df1 = df[df['Week'] > excld_wk]
df1 = df[df['Week'] != excld_wk] #-- Updated on 1/10/2024
del df

# Droping 'Week' Column
df1.drop(labels=['Week'], axis=1, inplace=True)

# Saving to CSV File
df1.to_csv('SHL_3weeks_Data.csv', index=False, float_format='%.2f')

####-------------------- PART-II: Processing -----------------------------------

#### Load Data
# Load up all data
df = pd.read_csv('SHL_3weeks_Data.csv')
df = df.fillna(0)

#### Preprocessing
# Changing Data Structure by Turbine ID
df_date = df.iloc[0:, 0:1] # date_time
date_list = df_date.columns.values.tolist()

# Converting str to DateTime format
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])


#### Extracting Turbine ID
num_of_tags = 20  # number of tags to query per turbine

end = 1
df_concat =[]
for i in range(1, 51):  
    # Chopping
    start = end
    end = start + num_of_tags
    df_tag = df.iloc[0:, start:end]

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
            replace_col_name = column_name[4:end]
            new_col_name.append(replace_col_name)    
    
    # replacing with new column names
    df_tag.columns = [new_col_name]

    # merging date part and tag part
    df_unit = df_date.join(df_tag)

    # making all column lists
    all_lists = date_list + new_col_name
    df_unit.columns = [all_lists]

    if i == 1:
        df_concat = df_unit
        frames = df_concat.copy()
    else:
        # appending all turbine data
        df_concat = pd.concat([frames, df_unit])
        frames = df_concat.copy()
        
## Fix for multi Index issue
df_concat.columns = df_concat.columns.map('_'.join)
del df

df = df_concat.copy()
del df_concat, df_unit, df_tag, all_lists, frames


#### Calendar Info
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
df['year_Date'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.year
df['quarter_Date'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.quarter
df['month_Date'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.month
df['week_Date'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.isocalendar().week
df['day_Date'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.day
df['dayofweek_Date'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.dayofweek
df['dayofyear_Date'] = pd.to_datetime(df['TimeStamp'], format='%y-%m-%d').dt.dayofyear
df['hour_Date'] = pd.to_datetime(df['TimeStamp'], format='%H:%M:%S').dt.hour


#### Retain Only Three Weeks Data
week_num = df['week_Date'].unique()


#### Health Score Generation Using Temperature
# Gen Brg-1 Temp
GnBrg1_fleet_avg = df.groupby(['week_Date']).mean()[['GnBrg1']].reset_index()
GnBrg1_fleet_avg.columns = ['Week', 'GnBrg1_Fleet_Avg_Temp']

week_num = []
for j in range(0, len(GnBrg1_fleet_avg.Week.unique())):
    week_num.append(j+1)   
for i in range(0, len(GnBrg1_fleet_avg)):
    col_name = 'Week_' + str(week_num[i]) + '_GnBrg1_Fleet_Avg_Temp'
    df[col_name] = GnBrg1_fleet_avg.iloc[i,1]
del col_name, i

# Gen Brg-2 Temp
GnBrg2_fleet_avg = df.groupby(['week_Date']).mean()[['GnBrg2']].reset_index()
GnBrg2_fleet_avg.columns = ['Week', 'GnBrg2_Fleet_Avg_Temp']
for i in range(0, len(GnBrg2_fleet_avg)):
    col_name = 'Week_' + str(week_num[i]) + '_GnBrg2_Fleet_Avg_Temp'
    df[col_name] = GnBrg2_fleet_avg.iloc[i,1]
del col_name, i

# Gearbox Oil Temp
GbxOil_fleet_avg = df.groupby(['week_Date']).mean()[['TrmTmpGbxOil']].reset_index()
GbxOil_fleet_avg.columns = ['Week', 'GbxOil_Fleet_Avg_Temp']
for i in range(0, len(GbxOil_fleet_avg)):
    col_name = 'Week_' + str(week_num[i]) + '_GbxOil_Fleet_Avg_Temp'
    df[col_name] = GbxOil_fleet_avg.iloc[i,1]
del col_name, i

# Shaft Bearing-1 Temp
ShfBrg1_fleet_avg = df.groupby(['week_Date']).mean()[['TrmTmpShfBrg1']].reset_index()
ShfBrg1_fleet_avg.columns = ['Week', 'ShfBrg1_Fleet_Avg_Temp']
for i in range(0, len(ShfBrg1_fleet_avg)):
    col_name = 'Week_' + str(week_num[i]) + '_ShfBrg1_Fleet_Avg_Temp'
    df[col_name] = ShfBrg1_fleet_avg.iloc[i,1]
del col_name, i

# Shaft Bearing-2 Temp
ShfBrg2_fleet_avg = df.groupby(['week_Date']).mean()[['TrmTmpShfBrg2']].reset_index()
ShfBrg2_fleet_avg.columns = ['Week', 'ShfBrg2_Fleet_Avg_Temp']
for i in range(0, len(ShfBrg2_fleet_avg)):
    col_name = 'Week_' + str(week_num[i]) + '_ShfBrg2_Fleet_Avg_Temp'
    df[col_name] = ShfBrg2_fleet_avg.iloc[i,1]
del col_name, i

# Hub Temp
Hub_fleet_avg = df.groupby(['week_Date']).mean()[['HubTmp']].reset_index()
Hub_fleet_avg.columns = ['Week', 'Hub_Fleet_Avg_Temp']
for i in range(0, len(Hub_fleet_avg)):
    col_name = 'Week_' + str(week_num[i]) + '_Hub_Fleet_Avg_Temp'
    df[col_name] = Hub_fleet_avg.iloc[i,1]
del col_name, i

# Rot Brg
RotBrg_fleet_avg = df.groupby(['week_Date']).mean()[['RotBrgTmp']].reset_index()
RotBrg_fleet_avg.columns = ['Week', 'RotBrg_Fleet_Avg_Temp']
for i in range(0, len(RotBrg_fleet_avg)):
    col_name = 'Week_' + str(week_num[i]) + '_RotBrg_Fleet_Avg_Temp'
    df[col_name] = RotBrg_fleet_avg.iloc[i,1]
del col_name, i

# Only Date Extraction without Time
df['Just_Date'] = df['TimeStamp'].dt.date


#### Reorder Tag Lists
reorder_list = ['TimeStamp','Turbine_ID','TotPF','CnvAirTmp','Torq','GnBrg1','GnBrg2','GnTmpSta','ExTmp','IntlTmp',
                'TopBoxTmp','WdSpd','WdSpd1','WdSpd2','HubTmp','RotBrgTmp','RotSpd','GbxSpd','TrmTmpGbxOil','TrmTmpShfBrg1',
                'TrmTmpShfBrg2','W','year_Date','quarter_Date','month_Date','week_Date','day_Date','dayofweek_Date',
                'dayofyear_Date','hour_Date','Week_1_GnBrg1_Fleet_Avg_Temp','Week_2_GnBrg1_Fleet_Avg_Temp',
                'Week_3_GnBrg1_Fleet_Avg_Temp','Week_1_GnBrg2_Fleet_Avg_Temp','Week_2_GnBrg2_Fleet_Avg_Temp',
                'Week_3_GnBrg2_Fleet_Avg_Temp','Week_1_GbxOil_Fleet_Avg_Temp','Week_2_GbxOil_Fleet_Avg_Temp',
                'Week_3_GbxOil_Fleet_Avg_Temp','Week_1_ShfBrg1_Fleet_Avg_Temp','Week_2_ShfBrg1_Fleet_Avg_Temp',
                'Week_3_ShfBrg1_Fleet_Avg_Temp','Week_1_ShfBrg2_Fleet_Avg_Temp','Week_2_ShfBrg2_Fleet_Avg_Temp',
                'Week_3_ShfBrg2_Fleet_Avg_Temp','Week_1_Hub_Fleet_Avg_Temp','Week_2_Hub_Fleet_Avg_Temp',
                'Week_3_Hub_Fleet_Avg_Temp','Week_1_RotBrg_Fleet_Avg_Temp','Week_2_RotBrg_Fleet_Avg_Temp',
                'Week_3_RotBrg_Fleet_Avg_Temp','Just_Date']
df = df[reorder_list]


#### Saving to File -- 'df'
## Save to a Desination Folder
df.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/SHL_Processed_Data_v2.csv', index=False, float_format='%.2f')



####----------------------- Weekly Report ----------------------------
# Changing Column Orders
useful_col = ['TimeStamp',
              'Turbine_ID',
              'WdSpd',
              'W',
              'year_Date',
              'quarter_Date',
              'month_Date',
              'week_Date',
              'day_Date',
              'dayofweek_Date',
              'dayofyear_Date',
              'hour_Date']

df1 = df[useful_col]
del df

# Filtering Out only the Latest Week Data
week_num = df1['week_Date'].unique()
wk_num = week_num[len(week_num)-1]
df1 = df1[df1['week_Date'] == wk_num]

## Net Total Calculation
# Net Total, kW
net_total_kW = df1.groupby(['Turbine_ID']).sum()[['W']].reset_index()
net_total_kW.columns =['Turbine_ID', 'Net Total, kW']

# Net Total, MWh
net_total_MWh = (df1.groupby(['Turbine_ID']).sum()[['W']]/6000).reset_index()
net_total_MWh.columns =['Turbine_ID', 'Net Total, MWh']
df_weekly = pd.merge(net_total_kW, net_total_MWh, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Net Total, kW'] = df_weekly['Net Total, kW'].replace(np.nan, 0)    
df_weekly['Net Total, MWh'] = df_weekly['Net Total, MWh'].replace(np.nan, 0)

# Net Average, KWh
net_average_kWh = (df1.groupby(['Turbine_ID']).mean()[['W']]/6).reset_index()
net_average_kWh.columns =['Turbine_ID', 'Net Avg, kWh']
df_weekly = pd.merge(df_weekly, net_average_kWh, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Net Avg, kWh'] = df_weekly['Net Avg, kWh'].replace(np.nan, 0)

## Gross Total Calculation
# Gross Total, kW
df1_gross = df1[df1['W'] > 0]
gross_total_kW = df1_gross.groupby(['Turbine_ID']).sum()[['W']].reset_index()
gross_total_kW.columns =['Turbine_ID', 'Gross Total, kW']
df_weekly = pd.merge(df_weekly, gross_total_kW, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Gross Total, kW'] = df_weekly['Gross Total, kW'].replace(np.nan, 0)

# Gross Total, MWh
gross_total_MWh = (df1_gross.groupby(['Turbine_ID']).sum()[['W']]/6000).reset_index()
gross_total_MWh.columns =['Turbine_ID', 'Gross Total, MWh']
df_weekly = pd.merge(df_weekly, gross_total_MWh, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Gross Total, MWh'] = df_weekly['Gross Total, MWh'].replace(np.nan, 0)

# Gross Average, KWh
gross_average_kWh = (df1_gross.groupby(['Turbine_ID']).mean()[['W']]/6).reset_index()
gross_average_kWh.columns =['Turbine_ID', 'Gross Avg, kWh']
df_weekly = pd.merge(df_weekly, gross_average_kWh, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Gross Avg, kWh'] = df_weekly['Gross Avg, kWh'].replace(np.nan, 0)

## Parasitic Total Calculation
# Parasitic Total, kW
df1_parasite = df1[df1['W'] <= 0]
parasite_total_kW = df1_parasite.groupby(['Turbine_ID']).sum()[['W']].reset_index()
parasite_total_kW.columns =['Turbine_ID', 'Parasite Total, kW']
df_weekly = pd.merge(df_weekly, parasite_total_kW, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Parasite Total, kW'] = df_weekly['Parasite Total, kW'].replace(np.nan, 0)

# Parasitic Total, MWh
parasite_total_MWh = (df1_parasite.groupby(['Turbine_ID']).sum()[['W']]/6000).reset_index()
parasite_total_MWh.columns =['Turbine_ID', 'Parasite Total, MWh']
df_weekly = pd.merge(df_weekly, parasite_total_MWh, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Parasite Total, MWh'] = df_weekly['Parasite Total, MWh'].replace(np.nan, 0)

# Parasitic Average, kWh
parasite_average_kWh = (df1_parasite.groupby(['Turbine_ID']).mean()[['W']]/6).reset_index()
parasite_average_kWh.columns =['Turbine_ID', 'Parasite Avg, kWh']
df_weekly = pd.merge(df_weekly, parasite_average_kWh, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Parasite Avg, kWh'] = df_weekly['Parasite Avg, kWh'].replace(np.nan, 0)

## Dollar ($) Figure Calculation
# Net Income, MWh --> $
dollar_per_mwh = 90.00

# Two-Decimal Point format
pd.options.display.float_format = "{:,.2f}".format
df_weekly['Net Total Income, $'] = df_weekly['Net Total, MWh'] * dollar_per_mwh

# Replacing NaN with zeros
df_weekly['Net Total Income, $'] = df_weekly['Net Total Income, $'].replace(np.nan, 0)

## Total Available Hours
wk_hrs = len(df1[df1['Turbine_ID']=='F01'])/6

hrs = pd.Series([wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,
              wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,
              wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,
              wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,
              wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs,wk_hrs])
df_weekly['Total Available Hours'] = hrs

## Turbine Runtime/Downtime, Hours
# total Run Hours
total_run_hour = (df1_gross.groupby(['Turbine_ID']).count()[['W']]/6).reset_index()
total_run_hour.columns =['Turbine_ID', 'Total Run Hours']
df_weekly = pd.merge(df_weekly, total_run_hour, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Total Run Hours'] = df_weekly['Total Run Hours'].replace(np.nan, 0)

# total Down Hours
total_down_hour = (df1_parasite.groupby(['Turbine_ID']).count()[['W']]/6).reset_index()
total_down_hour.columns =['Turbine_ID', 'Total Down Hours']
df_weekly = pd.merge(df_weekly, total_down_hour, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Total Down Hours'] = df_weekly['Total Down Hours'].replace(np.nan, 0)

# Percent Calculation
df_weekly['Run Hours, %'] = (df_weekly['Total Run Hours'] / df_weekly['Total Available Hours']) * 100
df_weekly['Down Hours, %'] = (df_weekly['Total Down Hours'] / df_weekly['Total Available Hours']) * 100

# Replacing NaN with zeros
df_weekly['Run Hours, %'] = df_weekly['Run Hours, %'].replace(np.nan, 0)
df_weekly['Down Hours, %'] = df_weekly['Down Hours, %'].replace(np.nan, 0)

## Wind Cut-In/Cut-Out, Hours
# Turbine Wind Cut-in
df1['Wind_Cut-In'] = df1['WdSpd'].apply(lambda x: 1 if (x >= 3 and x <= 24) else 0) # '1' = wind-blow, '0' = wind-stop
wind_cut_in = (df1.groupby(['Turbine_ID']).sum()[['Wind_Cut-In']]/6).reset_index()
wind_cut_in.columns =['Turbine_ID', 'Wind Cut-In, Hours']

df_weekly = pd.merge(df_weekly, wind_cut_in, how='outer', on=['Turbine_ID','Turbine_ID'])
df_weekly

# Turbine Wind Cut-out 
df1['Wind_Cut-Out'] = df1['WdSpd'].apply(lambda x: 0 if (x >= 3 and x <= 24) else 1) # '0' = wind-blow, '1' = wind-stop
wind_cut_out = (df1.groupby(['Turbine_ID']).sum()[['Wind_Cut-Out']]/6).reset_index()
wind_cut_out.columns =['Turbine_ID', 'Wind Cut-Out, Hours']

df_weekly = pd.merge(df_weekly, wind_cut_out, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing NaN with zeros
df_weekly['Wind Cut-In, Hours'] = df_weekly['Wind Cut-In, Hours'].replace(np.nan, 0)
df_weekly['Wind Cut-Out, Hours'] = df_weekly['Wind Cut-Out, Hours'].replace(np.nan, 0)

# Percent Calculation
df_weekly['Wind Cut-in Hours, %'] = (df_weekly['Wind Cut-In, Hours'] / df_weekly['Total Available Hours']) * 100
df_weekly['Wind Cut-out Hours, %'] = (df_weekly['Wind Cut-Out, Hours'] / df_weekly['Total Available Hours']) * 100

# Replacing NaN with zeros
df_weekly['Wind Cut-in Hours, %'] = df_weekly['Wind Cut-in Hours, %'].replace(np.nan, 0)
df_weekly['Wind Cut-out Hours, %'] = df_weekly['Wind Cut-out Hours, %'].replace(np.nan, 0)

## Wind Bucketization
# Bucketized Windspeed: Scenario-2
dfx = df1.copy()

def WdSpd_Bin(x):
    if x < 3.0:
        WdSpd_Bin = '0'
    elif x <= 3.5 and x >= 3.0:
        WdSpd_Bin =  '3'
    elif x <= 4.5 and x > 3.5:
        WdSpd_Bin =  '4'
    elif x <= 5.5 and x > 4.5:
        WdSpd_Bin =  '5'
    elif x <= 6.5 and x > 5.5:
        WdSpd_Bin =  '6'
    elif x <= 7.5 and x > 6.5:
        WdSpd_Bin =  '7'
    elif x <= 8.5 and x > 7.5:
        WdSpd_Bin =  '8'
    elif x <= 9.5 and x > 8.5:
        WdSpd_Bin =  '9'
    elif x <= 10.5 and x > 9.5:
        WdSpd_Bin =  '10'
    elif x <= 11.5 and x > 10.5:
        WdSpd_Bin =  '11'
    elif x <= 24 and x > 11.5:
        WdSpd_Bin =  '12-24'
    else:
        WdSpd_Bin =  '0'
    return WdSpd_Bin

dfx.loc[:,'WdSpd_Bin'] = dfx['WdSpd'].apply(lambda x: WdSpd_Bin(x))

del df1_gross, df1_parasite

wdspd_regime = ['0', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12-24']

dfx1 = dfx[dfx['WdSpd_Bin'] == wdspd_regime[0]]
result_wind_bin = dfx1.groupby(['Turbine_ID']).count()[['WdSpd_Bin']].reset_index()
result_wind_bin.columns =['Turbine_ID', 'Count_WdSpd_0_m/s']

# Merging
df_weekly = pd.merge(df_weekly, result_wind_bin, how='outer', on=['Turbine_ID','Turbine_ID'])

for i in range(1,len(wdspd_regime)):
    
    # Column Name Creation
    colmn_name = 'Count_WdSpd_' + str(wdspd_regime[i]) + '_m/s' 
    
    # Counting Number of Records
    dfx1 = dfx[dfx['WdSpd_Bin'] == wdspd_regime[i]]
    wind_bin_1 = dfx1.groupby(['Turbine_ID']).count()[['WdSpd_Bin']].reset_index()
    wind_bin_1.columns =['Turbine_ID', colmn_name]
    
    # Merging
    df_weekly = pd.merge(df_weekly, wind_bin_1, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing "NaN" with zeros
df_weekly['Count_WdSpd_0_m/s'] = df_weekly['Count_WdSpd_0_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_3_m/s'] = df_weekly['Count_WdSpd_3_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_4_m/s'] = df_weekly['Count_WdSpd_4_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_5_m/s'] = df_weekly['Count_WdSpd_5_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_6_m/s'] = df_weekly['Count_WdSpd_6_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_7_m/s'] = df_weekly['Count_WdSpd_7_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_8_m/s'] = df_weekly['Count_WdSpd_8_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_8_m/s'] = df_weekly['Count_WdSpd_8_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_9_m/s'] = df_weekly['Count_WdSpd_9_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_10_m/s'] = df_weekly['Count_WdSpd_10_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_11_m/s'] = df_weekly['Count_WdSpd_11_m/s'].replace(np.nan, 0)
df_weekly['Count_WdSpd_12-24_m/s'] = df_weekly['Count_WdSpd_12-24_m/s'].replace(np.nan, 0)    

# Percent in Wind Speed
Sum = (df_weekly['Count_WdSpd_0_m/s'] + df_weekly['Count_WdSpd_3_m/s'] + df_weekly['Count_WdSpd_4_m/s'] + 
       df_weekly['Count_WdSpd_4_m/s'] + df_weekly['Count_WdSpd_5_m/s'] + df_weekly['Count_WdSpd_6_m/s'] + 
       df_weekly['Count_WdSpd_7_m/s'] + df_weekly['Count_WdSpd_8_m/s'] + df_weekly['Count_WdSpd_9_m/s'] +
       df_weekly['Count_WdSpd_10_m/s'] + df_weekly['Count_WdSpd_11_m/s'] + df_weekly['Count_WdSpd_12-24_m/s'])

df_weekly['%_Count_WdSpd_0_m/s'] = (df_weekly['Count_WdSpd_0_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_3_m/s'] = (df_weekly['Count_WdSpd_3_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_4_m/s'] = (df_weekly['Count_WdSpd_4_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_5_m/s'] = (df_weekly['Count_WdSpd_5_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_6_m/s'] = (df_weekly['Count_WdSpd_6_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_7_m/s'] = (df_weekly['Count_WdSpd_7_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_8_m/s'] = (df_weekly['Count_WdSpd_8_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_9_m/s'] = (df_weekly['Count_WdSpd_9_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_10_m/s'] = (df_weekly['Count_WdSpd_10_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_11_m/s'] = (df_weekly['Count_WdSpd_11_m/s'] / Sum) * 100
df_weekly['%_Count_WdSpd_12-24_m/s'] = (df_weekly['Count_WdSpd_12-24_m/s'] / Sum) * 100

## Total Power Outputs by Wind Speed Buckets
wdspd_regime = ['3', '4', '5', '6', '7', '8', '9', '10', '11', '12-24']

dfx1 = dfx[dfx['WdSpd_Bin'] == wdspd_regime[0]]
result_power_output_bin = (dfx1.groupby(['Turbine_ID']).sum()[['W']]/1000).reset_index() # in MW
result_power_output_bin.columns =['Turbine_ID', 'TotalPwr@WdSpd=3_m/s']

# Merging
df_weekly = pd.merge(df_weekly, result_power_output_bin, how='outer', on=['Turbine_ID','Turbine_ID'])

for i in range(1,len(wdspd_regime)):
    
    # Column Name Creation
    colmn_name = 'TotalPwr@WdSpd=' + str(wdspd_regime[i]) + '_m/s' 
    
    # Counting Number of Records
    dfx1 = dfx[dfx['WdSpd_Bin'] == wdspd_regime[i]]
    total_pwr_at_wdspd_x = (dfx1.groupby(['Turbine_ID']).sum()[['W']]/1000).reset_index() # in MW
    total_pwr_at_wdspd_x.columns =['Turbine_ID', colmn_name]
    
    # Merging
    df_weekly = pd.merge(df_weekly, total_pwr_at_wdspd_x, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing "NaN" with zeros
df_weekly['TotalPwr@WdSpd=3_m/s'] = df_weekly['TotalPwr@WdSpd=3_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=4_m/s'] = df_weekly['TotalPwr@WdSpd=4_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=5_m/s'] = df_weekly['TotalPwr@WdSpd=5_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=6_m/s'] = df_weekly['TotalPwr@WdSpd=6_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=7_m/s'] = df_weekly['TotalPwr@WdSpd=7_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=8_m/s'] = df_weekly['TotalPwr@WdSpd=8_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=9_m/s'] = df_weekly['TotalPwr@WdSpd=9_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=10_m/s'] = df_weekly['TotalPwr@WdSpd=10_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=11_m/s'] = df_weekly['TotalPwr@WdSpd=11_m/s'].replace(np.nan, 0)
df_weekly['TotalPwr@WdSpd=12-24_m/s'] = df_weekly['TotalPwr@WdSpd=12-24_m/s'].replace(np.nan, 0)

# Percent in Power
Sum = (df_weekly['TotalPwr@WdSpd=3_m/s'] + df_weekly['TotalPwr@WdSpd=4_m/s'] + df_weekly['TotalPwr@WdSpd=5_m/s'] + 
       df_weekly['TotalPwr@WdSpd=6_m/s'] + df_weekly['TotalPwr@WdSpd=7_m/s'] + df_weekly['TotalPwr@WdSpd=8_m/s'] + 
       df_weekly['TotalPwr@WdSpd=9_m/s'] + df_weekly['TotalPwr@WdSpd=10_m/s'] + df_weekly['TotalPwr@WdSpd=11_m/s'] + 
       df_weekly['TotalPwr@WdSpd=12-24_m/s'])

df_weekly['%_TotalPwr@WdSpd=3_m/s'] = (df_weekly['TotalPwr@WdSpd=3_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=4_m/s'] = (df_weekly['TotalPwr@WdSpd=4_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=5_m/s'] = (df_weekly['TotalPwr@WdSpd=5_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=6_m/s'] = (df_weekly['TotalPwr@WdSpd=6_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=7_m/s'] = (df_weekly['TotalPwr@WdSpd=7_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=8_m/s'] = (df_weekly['TotalPwr@WdSpd=8_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=9_m/s'] = (df_weekly['TotalPwr@WdSpd=9_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=10_m/s'] = (df_weekly['TotalPwr@WdSpd=10_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=11_m/s'] = (df_weekly['TotalPwr@WdSpd=11_m/s'] / Sum) * 100
df_weekly['%_TotalPwr@WdSpd=12-24_m/s'] = (df_weekly['TotalPwr@WdSpd=12-24_m/s'] / Sum) * 100

# Replacing "NaN" with zeros
df_weekly['%_TotalPwr@WdSpd=3_m/s'] = df_weekly['%_TotalPwr@WdSpd=3_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=4_m/s'] = df_weekly['%_TotalPwr@WdSpd=4_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=5_m/s'] = df_weekly['%_TotalPwr@WdSpd=5_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=6_m/s'] = df_weekly['%_TotalPwr@WdSpd=6_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=7_m/s'] = df_weekly['%_TotalPwr@WdSpd=7_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=8_m/s'] = df_weekly['%_TotalPwr@WdSpd=8_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=9_m/s'] = df_weekly['%_TotalPwr@WdSpd=9_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=10_m/s'] = df_weekly['%_TotalPwr@WdSpd=10_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=11_m/s'] = df_weekly['%_TotalPwr@WdSpd=11_m/s'].replace(np.nan, 0)
df_weekly['%_TotalPwr@WdSpd=12-24_m/s'] = df_weekly['%_TotalPwr@WdSpd=12-24_m/s'].replace(np.nan, 0)

# Average Output by Each Wind Speed Regime
wdspd_range = [3.0, 3.1, 3.9, 4.1, 4.9, 5.1, 5.9, 6.1, 6.9, 7.1, 7.9, 8.1, 8.9, 9.1, 9.9, 10.1, 10.9, 11.1, 11.9, 12.1, 12.9, 24.1]
wdspd_point = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 24]

for i in range(0, int(len(wdspd_range)/2)):
    j = 2*i+1
    
    dfx1 = dfx[dfx['WdSpd'] >= wdspd_range[j-1]]
    dfx2 = dfx1[dfx1['WdSpd'] <= wdspd_range[j]]
    dfx2 = dfx2[dfx2['W'] > 0]
    
    # Column Name Creation
    colmn_name = 'Avg_Pwr_kw@WdSpd=' + str(wdspd_point[i])
    
    if i == 0:
        avg_pwr_at_wind_x = dfx2.groupby(['Turbine_ID']).mean()[['W']].reset_index()
        avg_pwr_at_wind_x.columns =['Turbine_ID', colmn_name]
        
        # Merging
        df_weekly = pd.merge(df_weekly, avg_pwr_at_wind_x, how='outer', on=['Turbine_ID','Turbine_ID'])
    
    else:
        avg_pwr_at_wind_x = dfx2.groupby(['Turbine_ID']).mean()[['W']].reset_index()
        avg_pwr_at_wind_x.columns =['Turbine_ID', colmn_name]   
        
        # Merging
        df_weekly = pd.merge(df_weekly, avg_pwr_at_wind_x, how='outer', on=['Turbine_ID','Turbine_ID'])
        
# Replacing "NaN" with zeros
df_weekly['Avg_Pwr_kw@WdSpd=3'] = df_weekly['Avg_Pwr_kw@WdSpd=3'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=4'] = df_weekly['Avg_Pwr_kw@WdSpd=4'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=5'] = df_weekly['Avg_Pwr_kw@WdSpd=5'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=6'] = df_weekly['Avg_Pwr_kw@WdSpd=6'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=7'] = df_weekly['Avg_Pwr_kw@WdSpd=7'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=8'] = df_weekly['Avg_Pwr_kw@WdSpd=8'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=9'] = df_weekly['Avg_Pwr_kw@WdSpd=9'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=10'] = df_weekly['Avg_Pwr_kw@WdSpd=10'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=11'] = df_weekly['Avg_Pwr_kw@WdSpd=11'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=12'] = df_weekly['Avg_Pwr_kw@WdSpd=12'].replace(np.nan, 0)
df_weekly['Avg_Pwr_kw@WdSpd=24'] = df_weekly['Avg_Pwr_kw@WdSpd=24'].replace(np.nan, 0)               

del dfx1, dfx2

## Forced Outage Hours
# 'Forced Outage Hours' and 'Wind Cutout Hours' calculation
df_weekly['Forced Outage Hours'] = df_weekly['Wind Cut-In, Hours'] - df_weekly['Total Run Hours']
df_weekly['Forced Outage Hours'] = df_weekly['Forced Outage Hours'] .apply(lambda x: x if x >= 0 else 0)

df_weekly['Forced Outage Hours, %'] = (df_weekly['Forced Outage Hours'] / df_weekly['Total Available Hours']) * 100

## Average Wind Speed for Each Turbine
df1_wind = df1[(df1['Wind_Cut-In'] == 1) | (df1['Wind_Cut-In'] == 0)]

# Getting turbine id names
turbine_list = df1_wind['Turbine_ID'].unique() # Turbine ID lists

turbine_id = []
wind_avg = []

# Calculating wind speed mean values and putting into a list
for i in range(0, len(turbine_list)): 
    tur_id = turbine_list[i]
    df1_wind_filt = df1_wind[df1_wind['Turbine_ID'] == tur_id]
    turbine_id.append(tur_id)
    wind_avg.append(df1_wind_filt['WdSpd'].mean())

# Putting into a dataframe
turbine_id = pd.Series(turbine_id, name = 'Turbine_ID')
wind_avg = pd.Series(wind_avg, name = 'Avg_WdSpd')
df_avg_wind = pd.concat([turbine_id, wind_avg], axis=1)

# Merging
df_weekly = pd.merge(df_weekly, df_avg_wind, how='outer', on=['Turbine_ID','Turbine_ID'])

# Replacing "NaN" with zeros
df_weekly['Avg_WdSpd'] = df_weekly['Avg_WdSpd'].replace(np.nan, 0)

del df1_wind

# Average Power Output (MWh) and Capacity Factor Calculation
weekly_hour = 168
nominal_output = 2.05

df_weekly['Average MWh'] = df_weekly['Gross Total, MWh'] / weekly_hour

# Replacing "NaN" with zeros
df_weekly['Average MWh'] = df_weekly['Average MWh'].replace(np.nan, 0)

df_weekly['Capacity Factor, %'] = (df_weekly['Average MWh'] / nominal_output) * 100

# Replacing "NaN" with zeros
df_weekly['Capacity Factor, %'] = df_weekly['Capacity Factor, %'].replace(np.nan, 0)

# Adding Starting Date & Ending Date
# Template for extracting Turbine ID
df1['time'] = pd.to_datetime(df1['TimeStamp'])
df1['dates'] = df1['time'].dt.date

x = df1.iloc[0:,-1:]

start_date = x.iloc[1, 0] # Extracting starting date
end_date = x.iloc[-1, 0] # Extracting ending date

df_weekly['Starting_Date'] = pd.Series([start_date for x in range(len(df_weekly.index))])
df_weekly['Ending_Date'] = pd.Series([end_date for x in range(len(df_weekly.index))])


#### Save to a Desination Folder
df_weekly.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/SHL_Weekly_Results.csv', index=False, float_format='%.2f')