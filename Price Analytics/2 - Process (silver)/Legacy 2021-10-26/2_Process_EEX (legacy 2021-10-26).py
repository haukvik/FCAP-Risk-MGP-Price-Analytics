# Databricks notebook source
# Defining input parameters
dbutils.widgets.text("p_ingest_date", "")
v_ingest_date = dbutils.widgets.get("p_ingest_date")

dbutils.widgets.text("p_datasource_name", "")
v_datasource_name = dbutils.widgets.get("p_datasource_name")

# COMMAND ----------

# MAGIC %run "../0 - Includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - Includes/common_functions"

# COMMAND ----------

# Defining parameters for pricing information
csv_path = f'/dbfs{bronze_folder_path}/{v_ingest_date}/{v_datasource_name}'
spot_filter = 'GasSpotMarketResults' # filter to identify spot price files
derivatives_filter = 'GasFutureMarketResults' # filter to identify derivatives price files

# COMMAND ----------

# Importing packages required to work with data
import pandas as pd # to workd with dataframes and csv files
import numpy as np # scientific calculations
import re # regex, for string/text functions
import glob # to work with directory structures and files
import os # general os tools
import shutil # necessary for moving files
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 1: PRICE DATA LOADING & WRANGLING

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPOT DATA LOADING

# COMMAND ----------

# Scanning for csv files containing spot prices
spot_filter = f'{csv_path}/{spot_filter}*.csv'
spot_files = glob.glob(spot_filter, recursive = True) # performing scan for relevant files

# Defining column names for spot CSV files
spot_colnames =['idx_code', 'location_name', 'product_name', 'start_date', 'end_date', 'price_mid', 'price_unit']

# COMMAND ----------

# Reading price files into dataframe
li = [] # initialising object to store price data
for spot_file in spot_files:
    try:
        df = pd.read_csv(spot_file, sep=";", header=None, usecols=[0,1,2,3,4,5,6], decimal=",", names=spot_colnames, comment='#')
    except:
        print('Error with this csv file:', csv_file)
    df = df.loc[df['idx_code'].isin(['ST', 'SP', 'IL'])] # select only price codes "SP" and "IL"
    df['pub_date'] = df.iloc[0,1] # reading publication day from the first row
    df['last_update'] = df.iloc[0,2] # reading last update from the first row
    df.drop(df[df['idx_code'] == 'ST'].index, inplace=True) # dropping unnecessary rows
    li.append(df) #adding the data to the price data object

spot_df = pd.concat(li, axis=0, ignore_index=True)
print(' -Spot prices loaded.')

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPOT DATA WRANGLING

# COMMAND ----------

# Convert price from string to float, keep at 4 decimals (safeguard)
spot_df['price_mid'] = pd.to_numeric(spot_df['price_mid'].apply(lambda x: re.sub(',', '.', str(x)))).round(4)

# New columns indicating D01 / WKN
spot_df['period_rel'] = spot_df['product_name'].copy(deep=True) # deep copy for new column instance
spot_df['period_rel'] = spot_df['period_rel'].apply(lambda x: 'WKD' if 'WEEKEND' in x else 'DA01')
spot_df['product_name'] = spot_df['product_name'].apply(lambda x: 'EGSI' if 'EGSI' in x else 'EOD')
spot_df.drop('idx_code', axis=1, inplace=True)

# Converting the unit; EEX quotes products as GBP although they are actually GBp
spot_df['price_unit'].replace("GBP/thm", "GBp/th", inplace=True)

# Formatting dates for entire dataset
spot_df['pub_date'] = spot_df['pub_date'].astype('datetime64[ns]')
spot_df['pub_date'] = spot_df['pub_date'].dt.normalize()
spot_df['start_date'] = spot_df['start_date'].astype('datetime64[ns]')
spot_df['start_date'] = spot_df['start_date'].dt.normalize()
spot_df['end_date'] = spot_df['end_date'].astype('datetime64[ns]')
spot_df['end_date'] = spot_df['end_date'].dt.normalize()
spot_df['last_update'] = spot_df['last_update'].astype('datetime64[ns]')

# Creating new column to indicate this is Spot (since it's spot data files we are loading)
spot_df['product_type'] = 'Spot'

spot_df['period_abs'] = '' # Spot products are relative, not absolute
spot_df['period_duration'] = spot_df['period_rel'].apply(lambda x: 'Day' if 'DA01' in x else 'Weekend')

# EEX Spot prices' end date is actually the day after delivery; fixing this:
spot_df['end_date'] = spot_df['end_date'].apply(lambda x: x - timedelta(1))

# Rearrange columns
spot_df = spot_df[['product_type', 'product_name', 'location_name', 'period_rel', 'period_abs', 'period_duration',
                   'pub_date', 'start_date', 'end_date', 'price_mid', 'price_unit', 'last_update']]
print(' -Spot prices cleaned.')

# COMMAND ----------

# MAGIC %md
# MAGIC ### FUTURES DATA LOADING
# MAGIC This follows a different structure than the spot data, and therefore needs to be done in a separate process.

# COMMAND ----------

# Scanning for relevant files
derivative_filter = f'{csv_path}/{derivatives_filter}*.csv' # defining search path & criteria
derivative_files = glob.glob(derivative_filter, recursive = True) # performing scan

# Defining column names for spot CSV files
fut_colnames =['idx_code', 'product_name', 'period_abs', 'start_date', 'end_date', 'price_mid', 'price_unit']

# Reading price files into dataframe
# Note: The data is very messy, duplicated, changes product code frequently etc, so there's a bit of cleaning here
li = [] # initialising object to store price data
for derivative_file in derivative_files:
    df = pd.read_csv(derivative_file, sep=";", header=None, decimal=",", comment='#', skiprows=8, usecols=[0,2,3,4,5,14,15], names=fut_colnames)
    df_pubinfo = pd.read_csv(derivative_file, sep=";", header=None, skiprows=7, nrows=1) # dataframe for collecting publication details from 1 row
    df['pub_date'] = df_pubinfo.iloc[0,1]
    df['last_update'] = df_pubinfo.iloc[0,2]
    df['location_name'] = derivative_file.rsplit("_")[-2] # Assigning the hub name of the product based on the file name
    li.append(df) #adding the data to the price data object

futures_df = pd.concat(li, axis=0, ignore_index=True)

print(' -Futures prices loaded.')

# COMMAND ----------

# MAGIC %md
# MAGIC ### FUTURES DATA WRANGLING
# MAGIC We chose to remove the non-priced products for simplicity, although they might be included in a future update (a lack of price may indicate low liquidity for instance and therefore contains information).

# COMMAND ----------

# Dropping NaN
futures_df.dropna(inplace=True)

# Subset on idx_code = PR & drop idx_code
futures_df = futures_df.loc[futures_df['idx_code'] == 'PR']
futures_df.drop('idx_code', axis=1, inplace=True)

# Dropping OTF futures and NR rows (duplicates)
futures_df.drop(futures_df.loc[futures_df['product_name'].str.contains('otf', case=False)].index, axis=0, inplace=True)
futures_df.drop(futures_df.loc[futures_df['product_name'].str.startswith('NR')].index, axis=0, inplace=True) # Dropping NR rows (duplicates)
futures_df['price_unit'].replace("GBP/thm", "GBP/therm", inplace=True)

# Convert price from string to float, keep at 4 decimals (safeguard)
futures_df['price_mid'] = pd.to_numeric(futures_df['price_mid'].apply(lambda x: re.sub(',', '.', str(x)))).round(4)

# Defining function to determine granularity of product
def find_granularity(row):
    # Keep the granularity of given price
    gran_product = row.rsplit(" ")[-2] # selecting second last word in column
    return gran_product

futures_df['period_duration'] = futures_df['product_name'].apply(find_granularity) # Setting the granularity (product duration)
futures_df['period_rel'] = '' # we do not know the relative period without a trading calendar
#futures_df['rel_period'] = futures_df['product'].apply(lambda x: )
futures_df['product_name'] = 'Settlement' # Defining these futures products as settlements
futures_df['product_type'] = 'Futures' # Making a column to indicate these are futures instruments

# Formatting dates
futures_df['pub_date'] = futures_df['pub_date'].astype('datetime64[ns]')
futures_df['pub_date'] = futures_df['pub_date'].dt.normalize()
futures_df['start_date'] = futures_df['start_date'].astype('datetime64[ns]')
futures_df['start_date'] = futures_df['start_date'].dt.normalize()
futures_df['end_date'] = futures_df['end_date'].astype('datetime64[ns]')
futures_df['end_date'] = futures_df['end_date'].dt.normalize()
futures_df['last_update'] = futures_df['last_update'].astype('datetime64[ns]')

# Restructuring columns and adding to dataframe for all eex prices:
futures_df = futures_df[['product_type', 'product_name', 'location_name', 'period_rel', 'period_abs', 'period_duration', 
                         'pub_date', 'start_date', 'end_date', 'price_mid', 'price_unit', 'last_update']]

print(' -Futures prices cleaned.')

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGING SPOT AND FUTURES PRICE DATA, ADD RELEVANT METADATA

# COMMAND ----------

# Concatenate dataframes, if they exist

try:
  spot_df
except:
  spot_df_exists = False
else:
  spot_df_exists = True

try:
  futures_df
except:
  futures_df_exists = False
else:
  futures_df_exists = True

if spot_df_exists and futures_df_exists:
  eex_df = pd.concat([spot_df, futures_df])
elif spot_df_exists:
  eex_df = spot_df
elif futures_df_exists:
  eex_df = futures_df
else:
  print('No data exists.')

# COMMAND ----------

# Add fixed details based on source:
eex_df['publication_name'] = 'EEX'
eex_df['data_source'] = v_datasource_name
eex_df['commodity_type'] = 'Natural Gas'
eex_df['price_bid'] = np.nan # Defining as NULL; Not available in source
eex_df['price_offer'] = np.nan # Defining as NULL; Not available in source
eex_df['ext_price_id'] = '' # Defining as NULL; no external ID

# Sometimes EEX hub naming is inconsistent, fixing:
#eex_df['location_name'].replace("Gaspool", "GPL", inplace=True)

# Splitting currency and delivery unit
eex_df['currency'] = eex_df['price_unit'].apply(lambda x: x.split("/")[0]) # selecting first element as currency
eex_df['unit'] = eex_df['price_unit'].apply(lambda x: x.split("/")[1]) # selecting second element as delivery unit
eex_df.drop('price_unit', axis=1, inplace=True)

# Sorting publication dates ascending:
eex_df = eex_df.sort_values(['pub_date', 'location_name', 'end_date', 'product_type', 'product_name'], ascending=[True, True, True, False, True])

# Create Unique Key to identify given price
eex_df['price_uk'] = (eex_df['publication_name'] + '/' 
                      + eex_df['commodity_type'] + '/' 
                      + eex_df['product_type'] + '/' 
                      + eex_df['product_name'] + '/' 
                      + eex_df['location_name'] + '/' 
                      + eex_df['pub_date'].astype(str) + '/' 
                      + eex_df['period_duration'].astype(str) + '/' 
                      + eex_df['end_date'].astype(str) + '/'
                      + eex_df['currency'] + '/'
                      + eex_df['unit'])

# Re-ordering columns
eex_df = eex_df[['price_uk', 'publication_name', 'data_source', 'commodity_type', 'product_type', 
                 'product_name', 'location_name', 'pub_date', 'period_rel', 'period_abs', 
                 'period_duration', 'start_date', 'end_date', 
                 'price_bid', 'price_mid', 'price_offer', 
                 'currency', 'unit', 'last_update', 'ext_price_id']]

# Resetting index
eex_df.reset_index(drop=True, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 2: Save prices to silver Delta Lake

# COMMAND ----------

# Convert to Spark DataFrame
eex_spark_df = spark.createDataFrame(eex_df)

# COMMAND ----------

eex_spark_df = add_ingestion_date(eex_spark_df)

# COMMAND ----------

# Write to Delta table
eex_spark_df.write.format("delta").mode("overwrite").saveAsTable("prices_processed.eex_prices")

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")
