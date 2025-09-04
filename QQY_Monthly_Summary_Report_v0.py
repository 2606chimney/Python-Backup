#!/usr/bin/env python


# QQY Monthly Reporting Analytics- 10/7/2024
# This reporting analytics utilizes "QQY_31days_KPIs_Processing_daily" file.
# Error Correction- Energy Consumption occurs an error when there is no data in an entire day- 11/4/2024


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
import datetime, dateutil.relativedelta
import datetime as dt

from numpy import mean
from pandas import DataFrame
from datetime import date, datetime, timedelta


#### Change Directory
Directory = 'C:/Users/Chongchan.Lee/SQL_Codes_Deployed/DailyTasks'
full_path = str(Directory) + '/' + 'QQY_KPIs_Processing_daily_p1.csv'


#### Read 31-Day Processed Data
df = pd.read_csv(full_path, encoding='unicode_escape')
df.fillna(0, inplace=True) # Added on 11/4/2024


#### Extract Previous Month Number
Today = date.today()
Prev_date = Today + dateutil.relativedelta.relativedelta(months=-1)
date_object = datetime.strptime(str(Prev_date), "%Y-%m-%d")
Month = date_object.month


#### Removing Non-relavant Month Data
df1= df[df['month'] == Month]
# Finding Unique Days in a Month
unique_dates = df1['day'].unique()
num_days = len(unique_dates)
num_hours = num_days * 24
starting_date = df1['TimeStamp'].iloc[0]
ending_date = df1['TimeStamp'].iloc[-1]


#### Creating Dataframe
result = {'Month':[Month], 'No. of Days':[num_days], 'Starting Date':[starting_date], 'Ending Date':[ending_date], 'Total Hours':[num_hours]}
Rcopy = pd.DataFrame(result)
Report = Rcopy.copy()


#### Engine Monthly Total MWh
engine_id = ['Engine01_daily_export, MWh','Engine02_daily_export, MWh','Engine03_daily_export, MWh',
             'Engine04_daily_export, MWh','Engine05_daily_export, MWh','Engine06_daily_export, MWh',
             'Engine07_daily_export, MWh','Engine08_daily_export, MWh','Engine09_daily_export, MWh',
             'Engine10_daily_export, MWh','Engine11_daily_export, MWh','Engine12_daily_export, MWh',
             'ST_daily_export, MWh']

Fleet_Monthly_Total_MWh = []
for j in engine_id:
    engine_monthly_mwh = []
    for i in unique_dates:
        df2 = df1[df1['day'] == i]
        engine_daily_mwh = round(df2[j].mean(), 2)
            
        if engine_daily_mwh < 0:
            engine_daily_mwh = 0
                
        engine_monthly_mwh.append(engine_daily_mwh)
        
    engine_total_monthly_mwh = round(sum(engine_monthly_mwh), 2)
    Fleet_Monthly_Total_MWh.append(engine_total_monthly_mwh)

Plant_Monthly_Total_MWh = sum(Fleet_Monthly_Total_MWh)
del engine_id, i, j, engine_monthly_mwh, df2, engine_daily_mwh, engine_total_monthly_mwh

## Writing Results: Total Export, MWh
engine_id = ['Engine01','Engine02','Engine03','Engine04','Engine05','Engine06','Engine07','Engine08','Engine09',
             'Engine10','Engine11','Engine12','ST']
j = 0
for i in Fleet_Monthly_Total_MWh:
    col_name = str(engine_id[j]) + ' Monthly Total Export, MWh'
    Report[col_name] = i
    j = j + 1

Report['Plant Mothly Total Export, MWh'] = Plant_Monthly_Total_MWh
del engine_id, i, j, col_name, Fleet_Monthly_Total_MWh, Plant_Monthly_Total_MWh


#### Engine Monthly Total Running Hours
engine_id = ['Engine01_daily_run, Hrs','Engine02_daily_run, Hrs','Engine03_daily_run, Hrs',
             'Engine04_daily_run, Hrs','Engine05_daily_run, Hrs','Engine06_daily_run, Hrs',
             'Engine07_daily_run, Hrs','Engine08_daily_run, Hrs','Engine09_daily_run, Hrs',
             'Engine10_daily_run, Hrs','Engine11_daily_run, Hrs','Engine12_daily_run, Hrs',
             'ST_daily_run, Hrs']

fleet_monthly_mean_run_hours = []
fleet_monthly_total_downtime_hours = []
fleet_monthly_total_run_rate = []
fleet_monthly_total_run_hours = []
for j in engine_id:
    engine_daily_hours = []
    for i in unique_dates:
        df2 = df1[df1['day'] == i]
        daily_hours = df2[j].mean()
        rounded_daily_hours = round(daily_hours, 1)
        engine_daily_hours.append(rounded_daily_hours)  
    engine_mean_run_hrs = round(mean(engine_daily_hours), 2)
    engine_total_run_hrs = round(sum(engine_daily_hours), 2)
    engine_total_run_rate = round(((engine_total_run_hrs / num_hours)*100), 2)
    engine_total_downtime_hrs = round((num_hours - engine_total_run_hrs), 2)   
    fleet_monthly_mean_run_hours.append(engine_mean_run_hrs)
    fleet_monthly_total_run_hours.append(engine_total_run_hrs)
    fleet_monthly_total_run_rate.append(engine_total_run_rate)
    fleet_monthly_total_downtime_hours.append(engine_total_downtime_hrs)

## Plant Total Run & Outage Hours
Plant_Monthly_Mean_Hours = round(mean(fleet_monthly_mean_run_hours), 2)
Plant_Monthly_Total_Hours = round(sum(fleet_monthly_total_run_hours), 2)
Plant_Monthly_Outage_Hours = round(sum(fleet_monthly_total_downtime_hours), 2)
Plant_Monthly_Running_Rate = round(((Plant_Monthly_Total_Hours/(num_hours*13))*100), 2)
del engine_id, i, j, df2, engine_daily_hours, daily_hours, rounded_daily_hours
del engine_mean_run_hrs, engine_total_run_hrs, engine_total_run_rate, engine_total_downtime_hrs
                                              
## Writing Results: Total Run Hours
Rcopy = Report.copy()
del Report
engine_id = ['Engine01','Engine02','Engine03','Engine04','Engine05','Engine06','Engine07','Engine08','Engine09',
             'Engine10','Engine11','Engine12','ST']

## Mean Run Hour
Report = Rcopy.copy()
del Rcopy
j = 0
for i in fleet_monthly_mean_run_hours:
    col_name = str(engine_id[j]) + ' Daily Mean Run Hours'
    Report[col_name] = i
    j = j + 1
    
# Total Run Hour
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
j = 0
for i in fleet_monthly_total_run_hours:
    col_name = str(engine_id[j]) + ' Monthly Total Run Hours'
    Report[col_name] = i
    j = j + 1

# Total Run Rate
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
j = 0
for i in fleet_monthly_total_run_rate:
    col_name = str(engine_id[j]) + ' Monthly Total Run Rates'
    Report[col_name] = i
    j = j + 1
    
# Total Down Hours
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
j = 0
for i in fleet_monthly_total_downtime_hours:
    col_name = str(engine_id[j]) + ' Monthly Total Outage Hours'
    Report[col_name] = i
    j = j + 1

for k in range(4):
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    if k == 0:
        Report['Plant Daily Mean Run Hours'] = Plant_Monthly_Mean_Hours
    elif k == 1:
        Report['Plant Monthly Total Run Hours'] = Plant_Monthly_Total_Hours
    elif k == 2:
        Report['Plant Total Outage Hours'] = Plant_Monthly_Outage_Hours
    else:
        Report['Plant Running Rate, %'] = Plant_Monthly_Running_Rate
del engine_id, i, j, col_name, Plant_Monthly_Mean_Hours, Plant_Monthly_Total_Hours
del Plant_Monthly_Outage_Hours, Plant_Monthly_Running_Rate 


#### Engine Monthly Total Running Hours by Fuel Types
fuel_type = ['_Gas, Hrs', '_Diesel, Hrs', '_HFO, Hrs']
engine_id = ['Engine01_daily_run','Engine02_daily_run','Engine03_daily_run','Engine04_daily_run',
             'Engine05_daily_run','Engine06_daily_run','Engine07_daily_run','Engine08_daily_run',
             'Engine09_daily_run','Engine10_daily_run','Engine11_daily_run','Engine12_daily_run']

Plant_Total_Monthly_RunHours_by_Fuel = []
Type = 0
for i in fuel_type:
    Type = Type + 1
    fuel_name = i[1:-5]

    engine_monthly_run_hrs_by_fuel = []
    for j in engine_id:
        engine_name = j + i

        engine_daily_hours = []
        for k in unique_dates:
            df2 = df1[df1['day'] == k]
            daily_hours = df2[str(engine_name)].mean()
            rounded_daily_hours = round(daily_hours, 4)
            engine_daily_hours.append(rounded_daily_hours)
            
        engine_total_fuel_hrs = round(sum(engine_daily_hours), 4)    
        engine_monthly_run_hrs_by_fuel.append(engine_total_fuel_hrs)

    if Type == 1:
        Engine_Monthly_Total_RunHours_Gas = engine_monthly_run_hrs_by_fuel
        total_hours = round(sum(engine_monthly_run_hrs_by_fuel), 4)
        Plant_Monthly_Total_Run_Gas = total_hours

    elif Type == 2:
        Engine_Monthly_Total_RunHours_Diesel = engine_monthly_run_hrs_by_fuel
        total_hours = round(sum(engine_monthly_run_hrs_by_fuel), 4)
        Plant_Monthly_Total_RunHours_Diesel = total_hours

    else:
        Engine_Monthly_Total_RunHours_HFO = engine_monthly_run_hrs_by_fuel
        total_hours = round(sum(engine_monthly_run_hrs_by_fuel), 4)
        Plant_Monthly_Total_RunHours_HFO = total_hours
        
    Plant_Total_Monthly_RunHours_by_Fuel.append(total_hours)
Plant_Total_Fuel_RunHours = round(sum(Plant_Total_Monthly_RunHours_by_Fuel), 4)

Monthly_Fuel_Run_Rate = []
for k in Plant_Total_Monthly_RunHours_by_Fuel:
    fuel_run_rate = round((k/Plant_Total_Fuel_RunHours), 4)
    Monthly_Fuel_Run_Rate.append(fuel_run_rate) # [Rate of Gas, Rate of Diesel, Rate of HFO]

del fuel_type, engine_id, Type, df2, fuel_name, engine_monthly_run_hrs_by_fuel, i, j, k, engine_name
del daily_hours, rounded_daily_hours, engine_daily_hours, engine_total_fuel_hrs, total_hours, fuel_run_rate

## Writing Results: Total Run Hours
engine_id = ['Engine01','Engine02','Engine03','Engine04','Engine05','Engine06','Engine07','Engine08','Engine09',
             'Engine10','Engine11','Engine12']

# Gas Total Run Hours by Engine
j = 0
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
for i in Engine_Monthly_Total_RunHours_Gas:
    col_name = str(engine_id[j]) + ' Monthly Run Hours, Gas'
    Report[col_name] = i
    j = j + 1
del i, j, col_name

# Diesel Total Run Hours by Engine
j = 0
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
for i in Engine_Monthly_Total_RunHours_Diesel:
    col_name = str(engine_id[j]) + ' Monthly Run Hours, Diesel'
    Report[col_name] = i
    j = j + 1
del i, j, col_name

# HFO Total Run Hours by Engine
j = 0
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
for i in Engine_Monthly_Total_RunHours_HFO:
    col_name = str(engine_id[j]) + ' Monthly Run Hours, HFO'
    Report[col_name] = i
    j = j + 1
del i, j, col_name
    
# Monthly Total Run Hours by Fuel
j = 1
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
for i in Plant_Total_Monthly_RunHours_by_Fuel:
    if j == 1:
        Report['Plant Total Run Hours, Gas'] = i
    elif j == 2:
        Report['Plant Total Run Hours, Diesel'] = i
    else:
        Report['Plant Total Run Hours, HFO'] = i
    j = j + 1
del i, j

# Plant Total Fuel Run Rates
Report['Plant Total Fuel Run Hours'] = Plant_Total_Fuel_RunHours

# Total Run Hours by Fuel
j = 1
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
for i in Monthly_Fuel_Run_Rate:
    if j == 1:
        Report['Plant Run Rates, Gas'] = i
    elif j == 2:
        Report['Plant Run Rates, Diesel'] = i
    else:
        Report['Plant Run Rates, HFO'] = i
    j = j + 1
del i, j


#### Monthly Heat Rate, kJ/kWh
engine_id = ['ST_daily_heatrate, kJ/kWh','BOP_Plant_Heat_Rate']
Plant_Monthly_Mean_HR =[]
for i in engine_id:
 
    asset_daily_hr = []
    for j in unique_dates:
        df2 = df1[df1['day'] == j]
        daily_hr = df2[i].mean()
        rounded_daily_hr = round(daily_hr, 2)
        asset_daily_hr.append(rounded_daily_hr)
    asset_monthly_mean_hr = round(mean(asset_daily_hr), 2)
    Plant_Monthly_Mean_HR.append(asset_monthly_mean_hr)
del engine_id, i, j, df2, daily_hr, rounded_daily_hr, asset_daily_hr, asset_monthly_mean_hr

## Writing Results: Total Export, MWh
j = 0
for i in Plant_Monthly_Mean_HR:
    j = j + 1
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    if j == 1:
        Report['ST Monthly Mean Heat Rate'] = i
    else:
        Report['BOP Monthly Mean Heat Rate'] = i
del i, j


#### Engine Monthly Efficiency, %
engine_id = ['Engine01_daily_efficiency, %','Engine02_daily_efficiency, %','Engine03_daily_efficiency, %',
             'Engine04_daily_efficiency, %','Engine05_daily_efficiency, %','Engine06_daily_efficiency, %',
             'Engine07_daily_efficiency, %','Engine08_daily_efficiency, %','Engine09_daily_efficiency, %',
             'Engine10_daily_efficiency, %','Engine11_daily_efficiency, %','Engine12_daily_efficiency, %',
             'ST_daily_efficiency, %', 'BOP_Plant_Efficiency']

Fleet_Monthly_Mean_Eff =[]
for i in engine_id:
 
    engine_daily_eff = []
    for j in unique_dates:
        df2 = df1[df1['day'] == j]
        daily_eff = df2[i].mean()
        rounded_daily_eff = round(daily_eff, 2)
        engine_daily_eff.append(rounded_daily_eff)
    engine_monthly_eff = round(mean(engine_daily_eff), 2)
    Fleet_Monthly_Mean_Eff.append(engine_monthly_eff)

Plant_Monthly_Mean_Eff = round(mean(Fleet_Monthly_Mean_Eff), 2)
del engine_id, i, j, df2, engine_daily_eff, daily_eff, rounded_daily_eff, engine_monthly_eff

## Writing Results: Total Run Hours
engine_id = ['Engine01 Monthly Efficiency, %','Engine02 Monthly Efficiency, %','Engine03 Monthly Efficiency, %',
             'Engine04 Monthly Efficiency, %','Engine05 Monthly Efficiency, %','Engine06 Monthly Efficiency, %',
             'Engine07 Monthly Efficiency, %','Engine08 Monthly Efficiency, %','Engine09 Monthly Efficiency, %',
             'Engine10 Monthly Efficiency, %','Engine11 Monthly Efficiency, %','Engine12 Monthly Efficiency, %',
             'ST Monthly Efficiency, %', 'BOP Plant Monthly Efficiency, %']

# Fleet Monthly Mean Efficiency, %
j = 0
for i in Fleet_Monthly_Mean_Eff:
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    Report[engine_id[j]] = i
    j = j + 1

# Plant Monthly Mean Efficiency, %
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Plant Monthly Mean Efficiency'] = Plant_Monthly_Mean_Eff
del i, j


#### Gas Engines' Monthly Total Gas Flow: 'Engine01_Cum_GasFlow_kg','Engine01_Cum_HFOFlow_kg', 'Engine01_Cum_LFOFlow_kg'
engine_id = ['Engine01_daily_flow_','Engine02_daily_flow_','Engine03_daily_flow_',
             'Engine04_daily_flow_','Engine05_daily_flow_','Engine06_daily_flow_',
             'Engine07_daily_flow_','Engine08_daily_flow_','Engine09_daily_flow_',
             'Engine10_daily_flow_','Engine11_daily_flow_','Engine12_daily_flow_']
fuel_type = ['Gas, Kg','HFO, Kg','LFO, Kg']

Type = 0
Plant_Fuel_Flow_Total_by_Type = []
for i in fuel_type:
    Type = Type + 1
    
    all_engine_monthly_total = []
    for j in engine_id:
        tag_name = str(j + i)
        
        engine_daily_total = []
        for k in unique_dates:
            df2 = df1[df1['day'] == k]
            daily_mean = df2[tag_name].mean()
            rounded_daily_mean = round(daily_mean, 2)
            engine_daily_total.append(rounded_daily_mean)
        engine_monthly_total = round(sum(engine_daily_total), 2)
        all_engine_monthly_total.append(engine_monthly_total)
        
    if Type == 1:
        Fuel_Flow_Monthly_Total_Gas = all_engine_monthly_total
        plant_monthly_total = round(sum(Fuel_Flow_Monthly_Total_Gas), 2)
        
    elif Type == 2:
        Fuel_Flow_Monthly_Total_HFO = all_engine_monthly_total
        plant_monthly_total = round(sum(Fuel_Flow_Monthly_Total_HFO), 2)
        
    else:
        Fuel_Flow_Monthly_Total_LFO = all_engine_monthly_total
        plant_monthly_total = round(sum(Fuel_Flow_Monthly_Total_LFO), 2)
        
    Plant_Fuel_Flow_Total_by_Type.append(plant_monthly_total)

Total_Fuel_Flow = round(sum(Plant_Fuel_Flow_Total_by_Type), 2)
Gas_Flow_Rate = round((Plant_Fuel_Flow_Total_by_Type[0]/Total_Fuel_Flow)*100, 3)
HFO_Flow_Rate = round((Plant_Fuel_Flow_Total_by_Type[1]/Total_Fuel_Flow)*100, 3)
LFO_Flow_Rate = round((Plant_Fuel_Flow_Total_by_Type[2]/Total_Fuel_Flow)*100, 3)
del engine_id, i, j, df2, fuel_type, Type, all_engine_monthly_total, tag_name, engine_daily_total
del daily_mean, rounded_daily_mean, engine_monthly_total, plant_monthly_total

## Writing Results: Total Run Hours
engine_id = ['Engine01','Engine02','Engine03','Engine04','Engine05','Engine06','Engine07','Engine08','Engine09',
             'Engine10','Engine11','Engine12']

# Gas Fuel Flow by Engine, kg
j = 0
for i in Fuel_Flow_Monthly_Total_Gas:
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    
    col_name = str(engine_id[j]) + ' Monthly Gas Flow, kg'
    Report[col_name] = i
    j = j + 1
del i, j, col_name

# HFO Fuel Flow by Engine, kg
j = 0
for i in Fuel_Flow_Monthly_Total_HFO:
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    
    col_name = str(engine_id[j]) + ' Monthly HFO Flow, kg'
    Report[col_name] = i
    j = j + 1
del i, j, col_name

# LFO Fuel Flow by Engine, kg
j = 0
for i in Fuel_Flow_Monthly_Total_LFO:
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    
    col_name = str(engine_id[j]) + ' Monthly LFO Flow, kg'
    Report[col_name] = i
    j = j + 1
del i, j, col_name, engine_id
    
# Engine Monthly Total Flow by Fuel, kg
j = 1
for i in Plant_Fuel_Flow_Total_by_Type:
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    
    if j == 1:
        Report['Plant Total Gas Flow, kg'] = i
    elif j == 2:
        Report['Plant Total HFO Flow, kg'] = i
    else:
        Report['Plant Total LFO Flow, kg'] = i
    j = j + 1
del i, j

# Plant Total Fuel Flow, kg
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
    
Report['Plant Total Fuel Flow, kg'] = Total_Fuel_Flow

# Plant Total Fuel Flow Rate, %
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Plant Total Gas Flow Rate, %'] = Gas_Flow_Rate

Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Plant Total HFO Flow Rate, %'] = HFO_Flow_Rate

Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Plant Total LFO Flow Rate, %'] = LFO_Flow_Rate


#### Monthly Gas Efficiency/Consumption: 'Engine01_Gas_Consumption, kJ/kWh'
engine_id = ['Engine01_Gas_Consumption','Engine02_Gas_Consumption','Engine03_Gas_Consumption',
             'Engine04_Gas_Consumption','Engine05_Gas_Consumption','Engine06_Gas_Consumption',
             'Engine07_Gas_Consumption','Engine08_Gas_Consumption','Engine09_Gas_Consumption',
             'Engine10_Gas_Consumption','Engine11_Gas_Consumption','Engine12_Gas_Consumption']

Engine_Monthly_Total_Consumed = []
for j in engine_id:
    
    engine_daily_consumed = []
    for i in unique_dates:
        df2 = df1[df1['day'] == i]
        daily_consumed = df2[j].mean()
        rounded_daily_consumed = round(daily_consumed, 1)
        engine_daily_consumed.append(rounded_daily_consumed)

    engine_monthly_consumed = round(sum(engine_daily_consumed), 1)
    Engine_Monthly_Total_Consumed.append(engine_monthly_consumed)

Plant_Monthly_Total_Consumed = round(sum(Engine_Monthly_Total_Consumed), 1)    
del engine_id, i, j, df2, engine_daily_consumed, daily_consumed
del rounded_daily_consumed, engine_monthly_consumed


## Writing Results: Gas Consumption per Engine, kJ/kWh
engine_id = ['Engine01','Engine02','Engine03','Engine04','Engine05','Engine06','Engine07','Engine08','Engine09',
             'Engine10','Engine11','Engine12']

# Gas Consumption by Engine, kJ/kWh
j = 0
for i in Engine_Monthly_Total_Consumed:
    Rcopy = Report.copy()
    del Report
    Report = Rcopy.copy()
    del Rcopy
    
    col_name = str(engine_id[j]) + ' Gas Consumption, kJ/kWh'
    Report[col_name] = i
    j = j + 1  
Report['Plant Total Gas Consumption, kJ/kWh'] = Plant_Monthly_Total_Consumed
del i, j, col_name, engine_id


#### Boiler Total Steam Consumption: 'Boiler_TOTAL_STEAM_CONSUMPTION'
Boiler_Daily_Consumed_Calc = []
for i in unique_dates:
    df2 = df1[df1['day'] == i]
    df2 = df2.dropna(subset=['Boiler_TOTAL_STEAM_CONSUMPTION'])
    daily_consumed = round((df2.Boiler_TOTAL_STEAM_CONSUMPTION.iloc[-1] - df2.Boiler_TOTAL_STEAM_CONSUMPTION.iloc[0]),2)
    Boiler_Daily_Consumed_Calc.append(daily_consumed)

Plant_Monthly_Total_Steam_Consumed = round(sum(Boiler_Daily_Consumed_Calc), 2)
Plant_Monthly_Mean_Steam_Consumed = round(mean(Boiler_Daily_Consumed_Calc), 2)
del i, df2, daily_consumed

## Writing Results: Boiler Steam Consumption
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Monthly Total Boiler Steam Consumption'] = Plant_Monthly_Total_Steam_Consumed

Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Daily Average Boiler Steam Consumption'] = Plant_Monthly_Mean_Steam_Consumed


#### Boiler Steam Flow Calculation
mean_daily_steamflow = []
for i in unique_dates:
    df2 = df1[df1['day'] == i]
    daily_steamflow = df2['Boiler_STEAM_FLOW'].mean()
    rounded_daily_steamflow = round(daily_steamflow, 2)
    mean_daily_steamflow.append(rounded_daily_steamflow)

Monthly_Mean_Steam_Flow = round(mean(mean_daily_steamflow), 2)
Monthly_Total_Steam_Flow = round(sum(mean_daily_steamflow), 2)
del i, df2, mean_daily_steamflow, daily_steamflow, rounded_daily_steamflow

## Writing Results: Boiler Steam Consumption
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Boiler Monthly Total Steam Flow, kg/h'] = Monthly_Total_Steam_Flow

Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['Boiler daily Mean Steam Flow, kg/h'] = Monthly_Mean_Steam_Flow


#### ST Daily Steam Flow, kg/h
mean_st_daily_steamflow = []
for i in unique_dates:
    df2 = df1[df1['day'] == i]
    st_daily_steamflow = df2['ST_daily_steamflow, kg/h'].mean()
    rounded_st_daily_steamflow = round(st_daily_steamflow, 2)
    mean_st_daily_steamflow.append(rounded_st_daily_steamflow)

Monthly_ST_Mean_Steam_Flow = round(mean(mean_st_daily_steamflow), 2)
Monthly_ST_Total_Steam_Flow = round(sum(mean_st_daily_steamflow), 2)
del i, df2, mean_st_daily_steamflow, st_daily_steamflow, rounded_st_daily_steamflow

## Writing Results: Boiler Steam Consumption
Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['ST Monthly Total Steam Flow, kg/h'] = Monthly_ST_Total_Steam_Flow

Rcopy = Report.copy()
del Report
Report = Rcopy.copy()
del Rcopy
Report['ST Daily Mean Steam Flow, kg/h'] = Monthly_ST_Mean_Steam_Flow


#### Saving to File -- 'Report'
Report.to_csv('QQY_Monthly_Summary_Report.csv', index=False)
# Saving the Results
final_result_file_name = 'QQY_Monthly_Summary_Report.csv'
final_direct = str('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/VTScadaQuery/ROC_Monthly_Report/') + final_result_file_name
Report.to_csv(final_direct, index=False)


