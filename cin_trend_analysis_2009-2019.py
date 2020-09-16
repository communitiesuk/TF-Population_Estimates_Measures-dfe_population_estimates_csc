# -*- coding: utf-8 -*-
"""
Created on Fri Sep 11 17:14:59 2020

@title: CIN Population Estimates (2009-2019)
@author: pthewlis
"""

import pyodbc
import pandas as pd
import os
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# Change pandas options
pd.options.display.max_columns = 50 
pd.options.display.max_rows = 500    
pd.options.display.max_colwidth = 100
pd.options.display.precision = 2

# Dataset and computer information (Write a small program to provide information on the dataset)
def datainf():
    system_date = datetime.datetime.now()
    print("Dataset was last loaded on ", system_date.strftime("%d-%M-%Y %H:%M:%S"))
    return datainf # need to state what we are returning - otherwise it can't find it and is reported as None
print(datainf())





#*********************************************************
# Import data for SQL to Python
#*********************************************************

# Connect Python to SQL Database
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DAP-SQL01;'
                      'Database=HTF_TroubledFamiliesLAA;'
                      'Trusted_Connection=yes;')

sql_query = "SELECT * FROM [CIN_2009-2019]"
df = pd.read_sql(sql_query, conn)
areas = df.LA.unique()









#********************************************************************
# Functions for Data Cleaning, Processing and Handling Missing Data
#********************************************************************

# Function to Change Value Labels across Multiple Datasets Simultaneously
def renamevalues(dataframes,variable,oldvaluenames,newvaluenames):
    '''
    Loop through multiple datasets (in tuple datastructure), and then iterate through two lists in parallel
    '''
    for d in (dataframes):
        for(i,j) in zip(oldvaluenames,newvaluenames):
            d[variable] = d[variable].replace([i],j)
            
# Function to Convert Variables to Categorical Variablea across Multiple Datasets Simultaneously 
def categ_multidf(dataframes,variables_list):
    for d in (dataframes):
        for i in variables_list:
            d[i] = d[i].astype('category')
            
# Function for Creating Multiple Datasets for each Region
def dfregion(dataframe):
    regions = dataframe['Region'].unique().tolist()
    regions_copy = regions
    regions_lowercase = [x.lower() for x in regions_copy]
    regions_lowercase = [z.replace(" ", "_") for z in regions_lowercase]
    dataframes = df.groupby("Region")
    for name, df2 in dataframes:
        globals()["df_" + name.lower().replace(" ","")] = df2
        
# National: Handling Missing Data: Find Unique Missing Data Values from Each Dataset and Compile into One List
missing_list = []
def missing_national(dataframe,variable_list):
    for v in variable_list:
        x = dataframe[v].unique().tolist()
        x = [i for i in x if not i.isdigit()]
        missing_list.append(x)
    missing_values = [y for x in missing_list for y in x]
    missing_values = set(missing_values)
    missing_values = list(missing_values)
    return missing_values

# Regional: Handling Missing Data: Find Unique Missing Data Values from Each Dataset and Compile into One List
missing_list = []
def missing_regional(dataframes):
    for d in (dataframes):
        x = d['Count'].unique().tolist()
        x = [i for i in x if not i.isdigit()]
        missing_list.append(x)
    missing_values = [y for x in missing_list for y in x]
    missing_values = set(missing_values)
    missing_values = list(missing_values)
    return missing_values




#*************************************************
# Calculating Population Measures
#*************************************************
    
# (1) Handling Missing Data
    
# List Variables with Missing Data
variables = ["2009_count","2010_count","2011_count","2012_count","2013_count","2014_count","2015_count","2016_count","2017_count","2018_count","2019_count",
             "2009_rate","2010_rate","2011_rate","2012_rate","2013_rate","2014_rate","2015_rate","2016_rate","2017_rate","2018_rate","2019_rate"]

# Identify None values and replace with NaN
df.replace(to_replace=[None], value=np.nan, inplace=True)

# Convert String to Numeric ('coerce' will transform the non-numeric values into NaN)
for i in variables:
    df[i] = pd.to_numeric(df[i], errors = 'coerce')

 
# (2) Calculate Population Measures (by Local Authority)
    
# LAs with Highest Number/Rate of CIN
df_highest= df[['LA', '2019_count']]
df_highest.sort_values(['2019_count']).tail(15)
df_highest= df[['LA', '2019_rate']]
df_highest.sort_values(['2019_rate']).tail(15)



# LAs with Lowest Number/Rate of CIN
df_lowest = df[['LA', '2019_count']]
df_lowest.sort_values(['2019_count']).head(15)
df_lowest = df[['LA', '2019_rate']]
df_lowest.sort_values(['2019_rate']).head(15)


# (3) Calculate Population Measures (National) - National average for rate of CIN
round(np.mean(df['2019_count']),2)
round(np.mean(df['2019_rate']),2)

# (4) Calculate Population Measures (Regional) - Average for Regions with Lowest and Highest Rates of CIN
reg_rates = df.groupby('Region').agg({'2019_count': np.mean}, sort=True)
reg_rates.sort_values('2019_count', ascending=False)

# Alternative method: Dictionary to specify aggregation functions for each series:
r = {'2019_count': 'mean', '2019_rate': 'mean'}
reg_rates = df.groupby('Region').agg(r)
reg_rates.sort_values('2019_count',ascending=False)
reg_rates.sort_values('2019_rate',ascending=False)











































#**********************************************
# Create regional datasets
#**********************************************

# Create individual datasets for the different regions
dfregion(df)
# Create individual datasets for the different regions
df['Region'].unique()
df_eastmids = df[df['Region'] == "East Midlands"]
df_eastengland = df[df['Region'] == "East of England"]
df_london = df[df['Region'] == "London"]
df_northeast = df[df['Region'] == "North East"]
df_northwest = df[df['Region'] == "North West"]
df_southeast = df[df['Region'] == "South East"]
df_southwest = df[df['Region'] == "South West"]
df_westmids = df[df['Region'] == "West Midlands"]
df_yorkshire = df[df['Region'] == "Yorkshire and the Humber"]



# Reshape data for each dataset (Count estimates)

# Variables to drop
drop_vars = ['LA_Code']

# East Midlands
df_eastmids = df_eastmids.drop(drop_vars, axis=1) # remove variables for reshaping
df_eastmids = pd.melt(df_eastmids,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_eastmids = df_eastmids.sort_values(by=["Year"])

# East of England
df_eastengland = df_eastengland.drop(drop_vars, axis=1) # remove variables for reshaping
df_eastengland = pd.melt(df_eastengland,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_eastengland = df_eastengland.sort_values(by=["Year"])

# London
df_london = df_london.drop(drop_vars, axis=1) # remove variables for reshaping
df_london = pd.melt(df_london,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_london = df_london.sort_values(by=["Year"])

# North East
df_northeast = df_northeast.drop(drop_vars, axis=1) # remove variables for reshaping
df_northeast = pd.melt(df_northeast,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_northeast = df_northeast.sort_values(by=["Year"])

# North West
df_northwest = df_northwest.drop(drop_vars, axis=1) # remove variables for reshaping
df_northwest = pd.melt(df_northwest,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_northwest = df_northwest.sort_values(by=["Year"])

# South East
df_southeast = df_southeast.drop(drop_vars, axis=1) # remove variables for reshaping
df_southeast = pd.melt(df_southeast,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_southeast = df_southeast.sort_values(by=["Year"])

# South West
df_southwest = df_southwest.drop(drop_vars, axis=1) # remove variables for reshaping
df_southwest = pd.melt(df_southwest,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_southwest = df_southwest.sort_values(by=["Year"])

# West Midlands
df_westmids = df_westmids.drop(drop_vars, axis=1) # remove variables for reshaping
df_westmids = pd.melt(df_westmids,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_westmids = df_westmids.sort_values(by=["Year"])

# Yorkshire
df_yorkshire = df_yorkshire.drop(drop_vars, axis=1) # remove variables for reshaping
df_yorkshire = pd.melt(df_yorkshire,
                  ["LA","Region"],
                  var_name="Year",
                  value_name="Count")
df_yorkshire = df_yorkshire.sort_values(by=["Year"])







#*****************************************
# Cleaning and Processing the Data
#*****************************************

# Check Variable Types for Each Regional Dataset
df_eastmids.info()
df_eastengland.info()
df_london.info()
df_northeast.info()
df_northwest.info()
df_southeast.info()
df_southwest.info()
df_westmids.info()
df_yorkshire.info()

# Change values of Year to year only and not _count (replace the suffix "_count")
datasets = [df_eastmids,df_eastengland,df_london,df_northeast,df_northwest,df_southeast,df_southwest,df_westmids,df_yorkshire]
variable = ['Year']
old_years = ["2009_count","2010_count","2011_count","2012_count","2013_count","2014_count","2015_count","2016_count","2017_count","2018_count","2019_count"]
new_years = ["2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019"]

# Functions for Data Cleaning
renamevalues(datasets,variable,old_years,new_years)

# Convert all strings to categorical variables (LA,Region)
categ_vars = ["LA","Region","Year"]
categ_multidf(datasets,categ_vars)

# Convert all strings to integers (Count) across all datasets
for d in(datasets):
    d['Count'] = d['Count'].astype(float)
    
    
# Get indexes where name column has value '2009'
eastmids_index_2009 = df_eastmids[df_eastmids['Year'] == "2009"].index
easteng_index_2009 = df_eastengland[df_eastengland['Year'] == "2009"].index
london_index_2009 = df_london[df_london['Year'] == "2009"].index
northeast_index_2009 = df_northeast[df_northeast['Year'] == "2009"].index
northwest_index_2009 = df_northwest[df_northwest['Year'] == "2009"].index
southeast_index_2009 = df_southeast[df_northeast['Year'] == "2009"].index
southwest_index_2009 = df_southwest[df_southwest['Year'] == "2009"].index
westmids_index_2009 = df_westmids[df_westmids['Year'] == "2009"].index
yorkshire_index_2009 = df_yorkshire[df_yorkshire['Year'] == "2009"].index


# Delete these row indexes from dataFrame
df_eastmids.drop(eastmids_index_2009, inplace=True)
df_eastengland.drop(eastmids_index_2009, inplace=True)
df_london.drop(eastmids_index_2009, inplace=True)
df_northeast.drop(eastmids_index_2009, inplace=True)
df_northwest.drop(eastmids_index_2009, inplace=True)
df_southeast.drop(eastmids_index_2009, inplace=True)
df_southwest.drop(eastmids_index_2009, inplace=True)
df_westmids.drop(eastmids_index_2009, inplace=True)
df_yorkshire.drop(eastmids_index_2009, inplace=True)

    
# Handling Null Values (value = 0 using fillna)
for d in (datasets):
    d['Count'] = d['Count'].fillna(0)




        




