#!/usr/bin/env python
# coding: utf-8

## Azura-Edo Before- & After- Major Overhaul Assessment Dashboard

# Azura-Edo- 11/11/2023
# 1. Universal version for GT-11, GT-12, and GT-13
# 2. Creating a 'true NewDateTime' column


#### Importing Python Packages
import pandas as pd
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


#### Three Runs, GT11, Gt12, & GT13
for i in range(3):
    if i == 0:
        #### Defining Unit Name
        Unit = 'GT11'
        
        # Data File
        data_filename = Unit + ' Data.xlsx'
        data_sheet_pre = '10 days Before MO'
        data_sheet_post = 'Current Data'

        # Tag Description
        short_tag_desc_filename = 'Azura_TagDesc_List_allGTs.xlsx'
        short_tag_sheet = Unit + '_List'

        # Saving File
        sanitized_file_pre = 'Sanitized_Data_Pre_MO.csv'
        sanitized_file_post = 'Sanitized_Data_Post_MO.csv'

        # Final Result
        final_result_file = Unit + '_Combined_Results.csv'
        
        #### Read 'Pre MO' data
        df_pre = pd.read_excel(data_filename, sheet_name = data_sheet_pre)
        
        # Remove headers and setup a column names
        df_pre.columns = [''] * len(df_pre.columns) # Deleting 'Unnamed' header
        df_pre.columns = df_pre.iloc[0] # Assign the first row as a column name
        df_pre = df_pre.drop(labels=[0,1,2], axis=0) # Remove the first to third row from the dataframe
        df1 = df_pre.reset_index() # Reset index number
        df_pre = df1.drop('index', axis=1) # Dropping 'index' column
        del df1
        
        # Split DataTime column from the table
        df1 = df_pre.iloc[:, 0] # Split the first column (Date) from the rest
        df1_datetime = pd.DataFrame(df1)
        df1_datetime.columns = ['datetime'] # Assign a new column name, 'DateTime'

        # Split rest of columns from the table
        df2 = df_pre.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

        # Combine two sub-tables into one
        df1_datetime = df1_datetime.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)

        df = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
        del df1_datetime, df1, df2, df_pre
        
        # Sanitize DateTime column
        DT = df['datetime']

        # Trimming 'datetime' strings
        DateTime = []
        for i in range(len(DT)):
            string = DT[i]
            short_str = string[0:19]
            DateTime.append(short_str)

        df['DateTime'] = DateTime

        # Move 'DateTime' column into front column
        Date = df['DateTime']
        df.drop(labels=['datetime'], axis=1, inplace=True)
        df.drop(labels=['DateTime'], axis=1, inplace=True)
        df.insert(0,'DateTime', Date)
        del DT, DateTime, Date
        
        # Read Tag Description List
        df_tag_desc = pd.read_excel(short_tag_desc_filename, sheet_name = short_tag_sheet)
        desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only

        # Read column names from the table
        col_list = df.columns.values.tolist()

        # Rename 'column codes' to 'column descriptions'
        for i in range(len(desc_list)):
            df.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
        del df_tag_desc, desc_list, col_list
        
        # Active Power in % Calculation
        maxpower = 168 # mw
        df['DWATT, %'] = (df['ACTIVE POWER'] / maxpower) * 100  # DWATT calculation in %
        df['DWATT >= 85%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 85 else 0)
        df['DWATT >= 70%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 70 else 0)
        
        # Read 'Start Date' and 'End Date'
        start_date = df['DateTime'].iloc[0]
        end_date = df['DateTime'].iloc[-1]
        
        # Calendar
        df1 = df.copy()
        df1['DateTime'] = pd.to_datetime(df['DateTime'])

        df1['Date'] = df1['DateTime'].dt.date
        df1['Year'] = df1['DateTime'].dt.year
        df1['Month'] = df1['DateTime'].dt.month
        df1['Day'] = df1['DateTime'].dt.day
        df1['Hour'] = df1['DateTime'].dt.hour

        df = df1.copy()
        df['NewDateTime'] = pd.date_range(start=start_date, end=end_date, periods=len(df))
        Col = df.pop('NewDateTime')
        df.insert(0, 'NewDateTime', Col)
        del df1
        
        # Saving Sanitized Data- Pre_MO
        df.to_csv(sanitized_file_pre, index=False, float_format='%.2f')
        
        #### Read 'Post MO' Data
        # Adjusting Post MO date
        post_start_date = df['DateTime'].iloc[0] + pd.Timedelta(days=11)
        post_end_date = df['DateTime'].iloc[-1] + pd.Timedelta(days=11)

        # Read Data
        df_pre = pd.read_excel(data_filename, sheet_name = data_sheet_post)

        # Remove headers and setup a column names
        df_pre.columns = [''] * len(df_pre.columns) # Deleting 'Unnamed' header
        df_pre.columns = df_pre.iloc[0] # Assign the first row as a column name
        df_pre = df_pre.drop(labels=[0,1,2], axis=0) # Remove the first to third row from the dataframe

        df1 = df_pre.reset_index() # Reset index number
        df_pre = df1.drop('index', axis=1) # Dropping 'index' column
        del df1

        # Split DataTime column from the table
        df1 = df_pre.iloc[:, 0] # Split the first column (Date) from the rest
        df1_datetime = pd.DataFrame(df1)
        df1_datetime.columns = ['datetime'] # Assign a new column name, 'DateTime'

        # Split rest of columns from the table
        df2 = df_pre.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

        # Combine two sub-tables into one
        df1_datetime = df1_datetime.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)

        df = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
        del df1_datetime, df1, df2, df_pre

        # Sanitize DateTime column
        DT = df['datetime']

        # Trimming 'datetime' strings
        DateTime = []
        for i in range(len(DT)):
            string = DT[i]
            short_str = string[0:19]
            DateTime.append(short_str)    
        df['DateTime'] = DateTime

        # Move 'DateTime' column into front column
        Date = df['DateTime']
        df.drop(labels=['datetime'], axis=1, inplace=True)
        df.drop(labels=['DateTime'], axis=1, inplace=True)
        df.insert(0,'DateTime', Date)
        del DT, DateTime, Date, 

        # Read Tag Description List
        df_tag_desc = pd.read_excel(short_tag_desc_filename, sheet_name = short_tag_sheet)
        desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only

        # Read column names from the table
        col_list = df.columns.values.tolist()

        # Rename 'column codes' to 'column descriptions'
        for i in range(len(desc_list)):
            df.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
        del df_tag_desc, desc_list, col_list

        # Active Power in % Calculation
        maxpower = 168 # mw
        df['DWATT, %'] = (df['ACTIVE POWER'] / maxpower) * 100  # DWATT calculation in %
        df['DWATT >= 85%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 85 else 0)
        df['DWATT >= 70%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 70 else 0)

        ## Read 'Start Date' and 'End Date'
        start_date = df['DateTime'].iloc[0]
        end_date = df['DateTime'].iloc[-1]

        # Calendar
        df1 = df.copy()
        df1['DateTime'] = pd.to_datetime(df['DateTime'])
        df1['Date'] = df1['DateTime'].dt.date
        df1['Year'] = df1['DateTime'].dt.year
        df1['Month'] = df1['DateTime'].dt.month
        df1['Day'] = df1['DateTime'].dt.day
        df1['Hour'] = df1['DateTime'].dt.hour

        df = df1.copy()
        df['NewDateTime'] = pd.date_range(start=post_start_date, end=post_end_date, periods=len(df))
        Col = df.pop('NewDateTime')
        df.insert(0, 'NewDateTime', Col)
        del df1

        # Saving Sanitized Data- Post_MO
        df.to_csv(sanitized_file_post, index=False, float_format='%.2f')
        
        #### Concatenating Two Dataframes¶
        df_pre_nan = pd.read_csv(sanitized_file_pre)
        df_post_nan = pd.read_csv(sanitized_file_post)

        # Replacing NaN with Zeros
        df_pre = df_pre_nan.fillna(0)
        df_post = df_post_nan.fillna(0)
        
        frames = [df_pre, df_post]
        df = pd.concat(frames)
        
        #### Saving Final Results fro GT11
        df.to_csv(final_result_file, index=False, float_format='%.2f')

    elif i == 1:
        #### Defining Unit Name
        Unit = 'GT12'
        
        # Data File
        data_filename = Unit + ' Data.xlsx'
        data_sheet_pre = '10 days Before MO'
        data_sheet_post = 'Current Data'

        # Tag Description
        short_tag_desc_filename = 'Azura_TagDesc_List_allGTs.xlsx'
        short_tag_sheet = Unit + '_List'

        # Saving File
#         sanitized_file_pre = Unit + '_Sanitized_Data_Pre_MO.csv'
#         sanitized_file_post = Unit + '_Sanitized_Data_Post_MO.csv'
        sanitized_file_pre = 'Sanitized_Data_Pre_MO.csv'
        sanitized_file_post = 'Sanitized_Data_Post_MO.csv'

        # Final Result
        final_result_file = Unit + '_Combined_Results.csv'
        
        #### Read 'Pre MO' data
        df_pre = pd.read_excel(data_filename, sheet_name = data_sheet_pre)
        
        # Remove headers and setup a column names
        df_pre.columns = [''] * len(df_pre.columns) # Deleting 'Unnamed' header
        df_pre.columns = df_pre.iloc[0] # Assign the first row as a column name
        df_pre = df_pre.drop(labels=[0,1,2], axis=0) # Remove the first to third row from the dataframe
        df1 = df_pre.reset_index() # Reset index number
        df_pre = df1.drop('index', axis=1) # Dropping 'index' column
        del df1
        
        # Split DataTime column from the table
        df1 = df_pre.iloc[:, 0] # Split the first column (Date) from the rest
        df1_datetime = pd.DataFrame(df1)
        df1_datetime.columns = ['datetime'] # Assign a new column name, 'DateTime'

        # Split rest of columns from the table
        df2 = df_pre.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

        # Combine two sub-tables into one
        df1_datetime = df1_datetime.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)

        df = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
        del df1_datetime, df1, df2, df_pre
        
        # Sanitize DateTime column
        DT = df['datetime']

        # Trimming 'datetime' strings
        DateTime = []
        for i in range(len(DT)):
            string = DT[i]
            short_str = string[0:19]
            DateTime.append(short_str)

        df['DateTime'] = DateTime

        # Move 'DateTime' column into front column
        Date = df['DateTime']
        df.drop(labels=['datetime'], axis=1, inplace=True)
        df.drop(labels=['DateTime'], axis=1, inplace=True)
        df.insert(0,'DateTime', Date)
        del DT, DateTime, Date
        
        # Read Tag Description List
        df_tag_desc = pd.read_excel(short_tag_desc_filename, sheet_name = short_tag_sheet)
        desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only

        # Read column names from the table
        col_list = df.columns.values.tolist()

        # Rename 'column codes' to 'column descriptions'
        for i in range(len(desc_list)):
            df.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
        del df_tag_desc, desc_list, col_list
        
        # Active Power in % Calculation
        maxpower = 168 # mw
        df['DWATT, %'] = (df['ACTIVE POWER'] / maxpower) * 100  # DWATT calculation in %
        df['DWATT >= 85%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 85 else 0)
        df['DWATT >= 70%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 70 else 0)
        
        # Read 'Start Date' and 'End Date'
        start_date = df['DateTime'].iloc[0]
        end_date = df['DateTime'].iloc[-1]
        
        # Calendar
        df1 = df.copy()
        df1['DateTime'] = pd.to_datetime(df['DateTime'])

        df1['Date'] = df1['DateTime'].dt.date
        df1['Year'] = df1['DateTime'].dt.year
        df1['Month'] = df1['DateTime'].dt.month
        df1['Day'] = df1['DateTime'].dt.day
        df1['Hour'] = df1['DateTime'].dt.hour

        df = df1.copy()
        df['NewDateTime'] = pd.date_range(start=start_date, end=end_date, periods=len(df))
        Col = df.pop('NewDateTime')
        df.insert(0, 'NewDateTime', Col)
        del df1
        
        # Saving Sanitized Data- Pre_MO
        df.to_csv(sanitized_file_pre, index=False, float_format='%.2f')
        
        #### Read 'Post MO' Data
        # Adjusting Post MO date
        post_start_date = df['DateTime'].iloc[0] + pd.Timedelta(days=11)
        post_end_date = df['DateTime'].iloc[-1] + pd.Timedelta(days=11)

        # Read Data
        df_pre = pd.read_excel(data_filename, sheet_name = data_sheet_post)

        # Remove headers and setup a column names
        df_pre.columns = [''] * len(df_pre.columns) # Deleting 'Unnamed' header
        df_pre.columns = df_pre.iloc[0] # Assign the first row as a column name
        df_pre = df_pre.drop(labels=[0,1,2], axis=0) # Remove the first to third row from the dataframe

        df1 = df_pre.reset_index() # Reset index number
        df_pre = df1.drop('index', axis=1) # Dropping 'index' column
        del df1

        # Split DataTime column from the table
        df1 = df_pre.iloc[:, 0] # Split the first column (Date) from the rest
        df1_datetime = pd.DataFrame(df1)
        df1_datetime.columns = ['datetime'] # Assign a new column name, 'DateTime'

        # Split rest of columns from the table
        df2 = df_pre.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

        # Combine two sub-tables into one
        df1_datetime = df1_datetime.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)

        df = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
        del df1_datetime, df1, df2, df_pre

        # Sanitize DateTime column
        DT = df['datetime']

        # Trimming 'datetime' strings
        DateTime = []
        for i in range(len(DT)):
            string = DT[i]
            short_str = string[0:19]
            DateTime.append(short_str)    
        df['DateTime'] = DateTime

        # Move 'DateTime' column into front column
        Date = df['DateTime']
        df.drop(labels=['datetime'], axis=1, inplace=True)
        df.drop(labels=['DateTime'], axis=1, inplace=True)
        df.insert(0,'DateTime', Date)
        del DT, DateTime, Date, 

        # Read Tag Description List
        df_tag_desc = pd.read_excel(short_tag_desc_filename, sheet_name = short_tag_sheet)
        desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only

        # Read column names from the table
        col_list = df.columns.values.tolist()

        # Rename 'column codes' to 'column descriptions'
        for i in range(len(desc_list)):
            df.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
        del df_tag_desc, desc_list, col_list

        # Active Power in % Calculation
        maxpower = 168 # mw
        df['DWATT, %'] = (df['ACTIVE POWER'] / maxpower) * 100  # DWATT calculation in %
        df['DWATT >= 85%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 85 else 0)
        df['DWATT >= 70%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 70 else 0)

        ## Read 'Start Date' and 'End Date'
        start_date = df['DateTime'].iloc[0]
        end_date = df['DateTime'].iloc[-1]

        # Calendar
        df1 = df.copy()
        df1['DateTime'] = pd.to_datetime(df['DateTime'])
        df1['Date'] = df1['DateTime'].dt.date
        df1['Year'] = df1['DateTime'].dt.year
        df1['Month'] = df1['DateTime'].dt.month
        df1['Day'] = df1['DateTime'].dt.day
        df1['Hour'] = df1['DateTime'].dt.hour

        df = df1.copy()
        df['NewDateTime'] = pd.date_range(start=post_start_date, end=post_end_date, periods=len(df))
        Col = df.pop('NewDateTime')
        df.insert(0, 'NewDateTime', Col)
        del df1

        # Saving Sanitized Data- Post_MO
        df.to_csv(sanitized_file_post, index=False, float_format='%.2f')
        
        #### Concatenating Two Dataframes¶
        df_pre_nan = pd.read_csv(sanitized_file_pre)
        df_post_nan = pd.read_csv(sanitized_file_post)

        # Replacing NaN with Zeros
        df_pre = df_pre_nan.fillna(0)
        df_post = df_post_nan.fillna(0)
        
        frames = [df_pre, df_post]
        df = pd.concat(frames)
        
        #### Saving Final Results fro GT11
        df.to_csv(final_result_file, index=False, float_format='%.2f')

    else:
        #### Defining Unit Name
        Unit = 'GT13'
        
        # Data File
        data_filename = Unit + ' Data.xlsx'
        data_sheet_pre = '10 days Before MO'
        data_sheet_post = 'Current Data'

        # Tag Description
        short_tag_desc_filename = 'Azura_TagDesc_List_allGTs.xlsx'
        short_tag_sheet = Unit + '_List'

        # Saving File
#         sanitized_file_pre = Unit + '_Sanitized_Data_Pre_MO.csv'
#         sanitized_file_post = Unit + '_Sanitized_Data_Post_MO.csv'
        sanitized_file_pre = 'Sanitized_Data_Pre_MO.csv'
        sanitized_file_post = 'Sanitized_Data_Post_MO.csv'

        # Final Result
        final_result_file = Unit + '_Combined_Results.csv'
        
        #### Read 'Pre MO' data
        df_pre = pd.read_excel(data_filename, sheet_name = data_sheet_pre)
        
        # Remove headers and setup a column names
        df_pre.columns = [''] * len(df_pre.columns) # Deleting 'Unnamed' header
        df_pre.columns = df_pre.iloc[0] # Assign the first row as a column name
        df_pre = df_pre.drop(labels=[0,1,2], axis=0) # Remove the first to third row from the dataframe
        df1 = df_pre.reset_index() # Reset index number
        df_pre = df1.drop('index', axis=1) # Dropping 'index' column
        del df1
        
        # Split DataTime column from the table
        df1 = df_pre.iloc[:, 0] # Split the first column (Date) from the rest
        df1_datetime = pd.DataFrame(df1)
        df1_datetime.columns = ['datetime'] # Assign a new column name, 'DateTime'

        # Split rest of columns from the table
        df2 = df_pre.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

        # Combine two sub-tables into one
        df1_datetime = df1_datetime.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)

        df = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
        del df1_datetime, df1, df2, df_pre
        
        # Sanitize DateTime column
        DT = df['datetime']

        # Trimming 'datetime' strings
        DateTime = []
        for i in range(len(DT)):
            string = DT[i]
            short_str = string[0:19]
            DateTime.append(short_str)

        df['DateTime'] = DateTime

        # Move 'DateTime' column into front column
        Date = df['DateTime']
        df.drop(labels=['datetime'], axis=1, inplace=True)
        df.drop(labels=['DateTime'], axis=1, inplace=True)
        df.insert(0,'DateTime', Date)
        del DT, DateTime, Date
        
        # Read Tag Description List
        df_tag_desc = pd.read_excel(short_tag_desc_filename, sheet_name = short_tag_sheet)
        desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only

        # Read column names from the table
        col_list = df.columns.values.tolist()

        # Rename 'column codes' to 'column descriptions'
        for i in range(len(desc_list)):
            df.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
        del df_tag_desc, desc_list, col_list
        
        # Active Power in % Calculation
        maxpower = 168 # mw
        df['DWATT, %'] = (df['ACTIVE POWER'] / maxpower) * 100  # DWATT calculation in %
        df['DWATT >= 85%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 85 else 0)
        df['DWATT >= 70%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 70 else 0)
        
        # Read 'Start Date' and 'End Date'
        start_date = df['DateTime'].iloc[0]
        end_date = df['DateTime'].iloc[-1]
        
        # Calendar
        df1 = df.copy()
        df1['DateTime'] = pd.to_datetime(df['DateTime'])

        df1['Date'] = df1['DateTime'].dt.date
        df1['Year'] = df1['DateTime'].dt.year
        df1['Month'] = df1['DateTime'].dt.month
        df1['Day'] = df1['DateTime'].dt.day
        df1['Hour'] = df1['DateTime'].dt.hour

        df = df1.copy()
        df['NewDateTime'] = pd.date_range(start=start_date, end=end_date, periods=len(df))
        Col = df.pop('NewDateTime')
        df.insert(0, 'NewDateTime', Col)
        del df1
        
        # Saving Sanitized Data- Pre_MO
        df.to_csv(sanitized_file_pre, index=False, float_format='%.2f')
        
        #### Read 'Post MO' Data
        # Adjusting Post MO date
        post_start_date = df['DateTime'].iloc[0] + pd.Timedelta(days=11)
        post_end_date = df['DateTime'].iloc[-1] + pd.Timedelta(days=11)

        # Read Data
        df_pre = pd.read_excel(data_filename, sheet_name = data_sheet_post)

        # Remove headers and setup a column names
        df_pre.columns = [''] * len(df_pre.columns) # Deleting 'Unnamed' header
        df_pre.columns = df_pre.iloc[0] # Assign the first row as a column name
        df_pre = df_pre.drop(labels=[0,1,2], axis=0) # Remove the first to third row from the dataframe

        df1 = df_pre.reset_index() # Reset index number
        df_pre = df1.drop('index', axis=1) # Dropping 'index' column
        del df1

        # Split DataTime column from the table
        df1 = df_pre.iloc[:, 0] # Split the first column (Date) from the rest
        df1_datetime = pd.DataFrame(df1)
        df1_datetime.columns = ['datetime'] # Assign a new column name, 'DateTime'

        # Split rest of columns from the table
        df2 = df_pre.iloc[:, 1:].astype(float) # Split the rest columns and convert data type to float

        # Combine two sub-tables into one
        df1_datetime = df1_datetime.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)

        df = pd.concat([df1_datetime, df2], axis=1) # Combining two data frame into one
        del df1_datetime, df1, df2, df_pre

        # Sanitize DateTime column
        DT = df['datetime']

        # Trimming 'datetime' strings
        DateTime = []
        for i in range(len(DT)):
            string = DT[i]
            short_str = string[0:19]
            DateTime.append(short_str)    
        df['DateTime'] = DateTime

        # Move 'DateTime' column into front column
        Date = df['DateTime']
        df.drop(labels=['datetime'], axis=1, inplace=True)
        df.drop(labels=['DateTime'], axis=1, inplace=True)
        df.insert(0,'DateTime', Date)
        del DT, DateTime, Date, 

        # Read Tag Description List
        df_tag_desc = pd.read_excel(short_tag_desc_filename, sheet_name = short_tag_sheet)
        desc_list = df_tag_desc['Tag_Desc'] # Read 'Tag Description' only

        # Read column names from the table
        col_list = df.columns.values.tolist()

        # Rename 'column codes' to 'column descriptions'
        for i in range(len(desc_list)):
            df.rename(columns = {col_list[i]: desc_list[i]}, inplace = True)
        del df_tag_desc, desc_list, col_list

        # Active Power in % Calculation
        maxpower = 168 # mw
        df['DWATT, %'] = (df['ACTIVE POWER'] / maxpower) * 100  # DWATT calculation in %
        df['DWATT >= 85%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 85 else 0)
        df['DWATT >= 70%'] = df['DWATT, %'].apply(lambda x: 1 if x >= 70 else 0)

        ## Read 'Start Date' and 'End Date'
        start_date = df['DateTime'].iloc[0]
        end_date = df['DateTime'].iloc[-1]

        # Calendar
        df1 = df.copy()
        df1['DateTime'] = pd.to_datetime(df['DateTime'])
        df1['Date'] = df1['DateTime'].dt.date
        df1['Year'] = df1['DateTime'].dt.year
        df1['Month'] = df1['DateTime'].dt.month
        df1['Day'] = df1['DateTime'].dt.day
        df1['Hour'] = df1['DateTime'].dt.hour

        df = df1.copy()
        df['NewDateTime'] = pd.date_range(start=post_start_date, end=post_end_date, periods=len(df))
        Col = df.pop('NewDateTime')
        df.insert(0, 'NewDateTime', Col)
        del df1

        # Saving Sanitized Data- Post_MO
        df.to_csv(sanitized_file_post, index=False, float_format='%.2f')
        
        #### Concatenating Two Dataframes¶
        df_pre_nan = pd.read_csv(sanitized_file_pre)
        df_post_nan = pd.read_csv(sanitized_file_post)

        # Replacing NaN with Zeros
        df_pre = df_pre_nan.fillna(0)
        df_post = df_post_nan.fillna(0)
        
        frames = [df_pre, df_post]
        df = pd.concat(frames)
        
        #### Saving Final Results fro GT11
        df.to_csv(final_result_file, index=False, float_format='%.2f')

print('Done')


# In[ ]:




