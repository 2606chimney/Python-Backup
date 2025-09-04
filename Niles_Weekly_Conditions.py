#!/usr/bin/env python

## Niles Weekly Analytics

# Niles- 05/24/2024
# 1. GT-1A, GT-1B, and ST
# Multiple data files exist in the folder- Process all the files but only the last file remains


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
import shutil, os, glob
import datetime as dt
from numpy import mean
from pandas import DataFrame
from datetime import date, datetime, timedelta


#### Finding Excel Data File
os.chdir('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Niles/Dashboards/HistoricData')
path = os.getcwd() 
csv_files = glob.glob(os.path.join(path, "*.xlsx"))

count = 1
for f in csv_files:

    # read the csv file
    df = pd.read_excel(f)
    data_filename = f.split("\\")[-1]

    #### Reading Data File
    sheet_num = 'Sheet2'
    df_xlsx = pd.read_excel(data_filename, sheet_name = sheet_num)


    #### Remove headers and setup a column names
    df1 = df_xlsx.copy()

    df1.columns = [''] * len(df1.columns) # Deleting 'Unnamed' header or row
    df2 = df1.drop(labels=[0], axis=0) # Remove the first row, which is blank row, from the dataframe
    df3 = df2.drop(labels=[1], axis=0) # Remove the first row, which is blank row, from the dataframe
    del df1, df2

    df3 = df3.reset_index() # Reset index number
    df = df3.drop('index', axis=1) # Dropping 'index' column
    df.columns = df.iloc[0] # Assign the first row as a column name
    df = df.drop(labels=[0], axis=0) # Dropping 'index' column
    del df3


    #### Assign column name to datetime column
    ## Split DataTime column from the table
    df1 = df.iloc[:, 0] # Split the first column (Date) from the rest
    df1_datetime = pd.DataFrame(df1)
    df1_datetime.columns = ['TimeStamp'] # Assign a new column name, 'DateTime'

    ## Split rest of columns from the table
    df2 = df.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

    ## Combine two sub-tables into one
    df1_datetime = df1_datetime.reset_index(drop=True)
    df2 = df2.reset_index(drop=True)
    df3 = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
    del df, df1_datetime, df1, df2

    ## Sort 'TimeStamp' column in ascending order
    df3['time'] = df3['TimeStamp'].apply(lambda x: x.value)

    start_time = df3['time'].iloc[0]
    end_time = df3['time'].iloc[-1]

    if start_time > end_time:
        df2 = df3.sort_values(by='time')
    else:
        df2 = df3
    del df3
    df2.drop(labels=['time'], axis=1, inplace=True)
    df = df2.copy()


    #### Read Tag Description List
    os.chdir('C:/Users/Chongchan.Lee/SQL_Codes_Deployed/Niles')
    df_tag_desc = pd.read_excel('Tag_Short_Desc_986.xlsx', sheet_name = 'Sheet1')
    desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only

    ## Read column names from the table
    col_list = df2.columns.values.tolist()

    ## Rename 'column codes' to 'column descriptions'
    for i in range(len(desc_list)):
        df2.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
    del df_tag_desc, desc_list, col_list


    #### Calendar
    df1 = df2.copy()
    df1['DateTime'] = pd.to_datetime(df2['DateTime'])

    df1['Date'] = df1['DateTime'].dt.date
    df1['Year'] = df1['DateTime'].dt.year
    df1['Month'] = df1['DateTime'].dt.month
    df1['Day'] = df1['DateTime'].dt.day
    df1['Hour'] = df1['DateTime'].dt.hour


    ####********* Saving Data File Name with TimeStamp *********####
    file_timestamp = df1['Date'].iloc[-1]
    # print(file_timestamp)
    new_file_name = 'Niles_Data_' + str(file_timestamp) + '.csv'

    os.chdir('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Niles/Documents/DataArchive')
    df.to_csv(new_file_name, index=False, float_format='%.2f')
    os.chdir('C:/Users/Chongchan.Lee/SQL_Codes_Deployed/Niles')
    del df
    # ####********************************************************####


    # Move 'DateTime' column into front column
    hour = df1['Hour']
    df1.drop(labels=['Hour'], axis=1, inplace=True)
    df1.insert(1,'Hour', hour)

    day = df1['Day']
    df1.drop(labels=['Day'], axis=1, inplace=True)
    df1.insert(1,'Day', day)

    month = df1['Month']
    df1.drop(labels=['Month'], axis=1, inplace=True)
    df1.insert(1,'Month', month)

    year = df1['Year']
    df1.drop(labels=['Year'], axis=1, inplace=True)
    df1.insert(1,'Year', year)

    Date = df1['Date']
    df1.drop(labels=['Date'], axis=1, inplace=True)
    df1.insert(1,'Date', Date)
    del df2


    #### Splitting Each Turbine Data
    df_date = df1.iloc[0:, 0:6] # date_time
    df_gtA = df1.iloc[0:, 6:429]
    df_gtB = df1.iloc[0:, 429:852]
    df_st = df1.iloc[0:, 852:]
    df_st.head()

    df_GTA = pd.concat([df_date, df_gtA], axis=1) # Combining two data frame into one
    df_GTB = pd.concat([df_date, df_gtB], axis=1) # Combining two data frame into one
    df_ST = pd.concat([df_date, df_st], axis=1) # Combining two data frame into one


    #### Saving Data
    os.chdir('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Niles/Dashboards')
    df_GTA.to_csv('Niles_clean_data_GT-A.csv', index=False, float_format='%.2f')
    df_GTB.to_csv('Niles_clean_data_GT-B.csv', index=False, float_format='%.2f')
    df_ST.to_csv('Niles_clean_data_ST.csv', index=False, float_format='%.2f')


    #### Moving Data File
    os.chdir('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Niles/Dashboards/HistoricData')
    new_direct = 'C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Niles/Documents/DataArchive'
    shutil.move(data_filename, new_direct)



