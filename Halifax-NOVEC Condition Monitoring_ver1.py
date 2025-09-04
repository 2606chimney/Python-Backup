#!/usr/bin/env python

# Halifax-NOVEC Condition Monitoring
# Halifax, Monthly Condition Monitoring Dashboard- Version-1
# Deployed on 12/09/2024


#### Importing Python Packages
import pandas as pd
import numpy as np
import math
import csv
import re
import operator
import sys
import shutil, os
import pyodbc
import pytz
import datetime
from numpy import mean
from pandas import DataFrame
from datetime import date, datetime, timedelta
from matplotlib import pyplot


#### Reading Multiple Sheets, Removing Empty Rows & Columns, and Concatenating Horizontally
Directory = 'C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Halifax/Documents'
file_list = os.listdir(Directory)
full_path = str(Directory) + '/' + file_list[0]

for i in range(5):
    if i == 0:
        df_cwpump = pd.read_excel(full_path, sheet_name = 'CW Pumps')
#         df_cwpump = pd.read_excel(full_path, sheet_name = 'CW Pumps', encoding='unicode_escape')
        # Remove rows
        # df_cwpump1 = df_cwpump.drop([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16])
        df_cwpump1 = df_cwpump.drop(range(0,17))
        df_cwpump1.reset_index(drop=True, inplace=True)
        # Remove columns
        df_cwpump2 = df_cwpump1.drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2'])
        df_cwpump2.rename(columns={'Tag Name': 'DateTime'}, inplace=True)
        del df_cwpump, df_cwpump1

    elif i == 1:
        df_fdfan = pd.read_excel(full_path, sheet_name = 'FD Fan')
        # Remove rows
        df_fdfan1 = df_fdfan.drop(range(0,17))
        df_fdfan1.reset_index(drop=True, inplace=True)
        # Remove columns
        df_fdfan2 = df_fdfan1.drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Tag Name'])
        del df_fdfan, df_fdfan1
        
    elif i == 2:
        df_idfan = pd.read_excel(full_path, sheet_name = 'ID Fan')
        # Remove rows
        df_idfan1 = df_idfan.drop(range(0,17))
        df_idfan1.reset_index(drop=True, inplace=True)
        # Remove columns
        df_idfan2 = df_idfan1.drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Tag Name'])
        del df_idfan, df_idfan1
        
    elif i == 3:
        df_afwp = pd.read_excel(full_path, sheet_name = 'A FWP')
        # Remove rows
        df_afwp1 = df_afwp.drop(range(0,17))
        df_afwp1.reset_index(drop=True, inplace=True)
        # Remove columns
        df_afwp2 = df_afwp1.drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Tag Name'])
        del df_afwp, df_afwp1
        
    else:
        df_tg = pd.read_excel(full_path, sheet_name = 'Turbine Generator')
        # Remove rows
        df_tg1 = df_tg.drop(range(0,17))
        df_tg1.reset_index(drop=True, inplace=True)
        # Remove columns
        df_tg2 = df_tg1.drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Tag Name'])
        del df_tg, df_tg1

# Concatenating multiple dataframes
df_tmp = pd.concat([df_cwpump2, df_fdfan2, df_idfan2, df_afwp2, df_tg2], axis=1)

# Replacing NaN with ZerosTurbine Generator
df = df_tmp.fillna(-99)
del df_cwpump2, df_fdfan2, df_idfan2, df_afwp2, df_tg2


#### Moving the Data File to Different Directory
new_path = 'C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Halifax/Storage'
new_direct = str(new_path) + '/' + file_list[0]
shutil.move(full_path, new_direct)

df1 = df.copy()
df1['DateTime'] = pd.to_datetime(df['DateTime'])

df1['Date'] = df1['DateTime'].dt.date
df1['Year'] = df1['DateTime'].dt.year
df1['Month'] = df1['DateTime'].dt.month
df1['Day'] = df1['DateTime'].dt.day
df1['Hour'] = df1['DateTime'].dt.hour

df = df1.copy()
del df1

final_direct = str('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Halifax/Dashboard/') + 'Halifax_batch_data.csv'
df.to_csv(final_direct, index=False, float_format='%.2f')
del df


#### Convert Column Tags into Short Names
os.chdir('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Halifax/Dashboard')

df = pd.read_csv('Halifax_batch_data.csv')

# Read Tag Description List
os.chdir('C:/Users/Chongchan.Lee/SQL_Codes_Deployed/Halifax')

df_tag_desc = pd.read_excel('Halifax_Short_Tag_List.xlsx', sheet_name = 'Sheet1')
short_list = df_tag_desc['Short Tag'] # Read 'Tag Description' only

# Read column names from the table
col_list = df.columns.values.tolist()

# Rename 'column codes' to 'column descriptions'
for i in range(len(short_list)):
    df.rename(columns = {col_list[i]: short_list[i]}, inplace = True)
del df_tag_desc, short_list, col_list


#### Save Data with Short Tag Column Names
os.chdir('C:/Users/Chongchan.Lee/OneDrive - PIC Group, Inc/ROC/Halifax/Dashboard')
df.to_csv('Halifax_batch_data_shortname.csv', index=False, float_format='%.2f') 

