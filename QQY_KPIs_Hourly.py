#!/usr/bin/env python
# coding: utf-8

# Quisqueya Hourly KPI Calculations with Hourly Samples- 5/02/2023


##### Import Python Packages
import pandas as pd
import numpy as np
import math
import csv
import re
import matplotlib.pyplot as plt
import operator
import sys
import pyodbc
import pytz
import dateutil.relativedelta
import warnings
warnings.filterwarnings('ignore')

from numpy import mean
from pandas import DataFrame
from datetime import date, datetime, timedelta
from iapws import IAPWS97


#### Hourly Query Configuration
# Finding Current Hour
today = datetime.today()
Current_hour = pd.to_datetime(today).floor('H')
Future_hour = Current_hour + timedelta(hours = 1)
# Changing UTC to EST time
time_change_start = timedelta(hours=4)
time_change_end = timedelta(hours=4)
# Start Time
start_datetime = Current_hour + time_change_start
# End Time
end_datetime = Future_hour + time_change_end


#### Importing Tag Lists
df_list = pd.read_excel('Query_Tag_Lists.xlsx', sheet_name='QQY_hourly')
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


#### Saving Query Data
dfx.to_csv('QQY_hourly_query.csv', index=False)


#### Importing Query Data
df = pd.read_csv('QQY_hourly_query.csv')


#### NaN filled with Zero
df2 = df.copy()
df = df2.fillna(0)
del df2


#### Converting Pressure in "bar" into "MPa"
df['Gen_13_STEAM_PRESS_ABS (MPa)'] = df['Gen_13_STEAM_PRESS_ABS'] * 0.1


#### Inlet Steam Enthalpy (kJ/kg) Calculation
enthalpy = []
for j in range(len(df['Gen_13_STEAM_PRESS_ABS (MPa)'])):
    
    p1 = df['Gen_13_STEAM_PRESS_ABS (MPa)'].iloc[j]
    t1 = df['Gen_13_AVG_INLET_STEAM_TEMP_KELVIN'].iloc[j]
    
    if (p1 != 0) and (t1 != 0):
        Steam = IAPWS97(P = p1, T = t1)
        enthalpy.append(Steam.h)
    else:
        enthalpy.append(0)
            
inlet_steam_enthalpy = pd.Series(enthalpy)
df['Inlet Steam Enthalpy (kJ/kg)'] = pd.DataFrame(inlet_steam_enthalpy)
df['Enthalpy Flow to ST (kJ/h)'] = df['Inlet Steam Enthalpy (kJ/kg)'] * df['Gen_13_STEAM_FLOW_TURBINE']


#### Steam Turbine Heat Rate
heatrate = []
for j in range(len(df['Gen_13_BAG131UP01PV'])):
    st_flow = df['Boilers_RCT901F002AV'].iloc[j]
    active_pwr = df['Gen_13_BAG131UP01PV'].iloc[j]
    st_enthalpy = df['Enthalpy Flow to ST (kJ/h)'].iloc[j]
    
    if (st_flow*100)/active_pwr < 0.35:
        heatrate.append(st_enthalpy/active_pwr)
    else:
        heatrate.append(0)

ST_heatrate = pd.Series(heatrate)
df['Steam Turbine Heat Rate (kJ/kWh)'] = pd.DataFrame(ST_heatrate)


#### Steam Turbine Efficiency
efficiency = []
for j in range(len(df['Steam Turbine Heat Rate (kJ/kWh)'])):
    st_hr = df['Steam Turbine Heat Rate (kJ/kWh)'].iloc[j]
   
    if st_hr > 0:
        efficiency.append(1/(st_hr/3600))
    else:
        efficiency.append(0)

ST_Efficiency = pd.Series(efficiency)
df['Steam Turbine Efficiency (%)'] = pd.DataFrame(ST_Efficiency)


#### Dropiing Tags
df.drop(['Gen_13_BAG131UP01PV','Boilers_RCT901F002AV','Gen_13_AVG_INLET_STEAM_TEMP_KELVIN'], axis=1, inplace=True)
df.drop(['Gen_13_STEAM_FLOW_TURBINE','Gen_13_STEAM_PRESS_ABS','Gen_13_STEAM_PRESS_ABS (MPa)'], axis=1, inplace=True)


#### Saving Hourly KPIs
df.to_csv('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaDataExport/QQY_Hourly_KPIs.csv', index=False, float_format='%.2f')