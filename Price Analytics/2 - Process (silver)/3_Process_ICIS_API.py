# Databricks notebook source
# MAGIC %md
# MAGIC # Process ICIS Assessments and Indices external data from Bronze XML files

# COMMAND ----------

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

# Defining paths for price files
esgm_path = f'{bronze_folder_path}/{v_ingest_date}/{v_datasource_name}'

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import lxml
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load ICIS XML files

# COMMAND ----------

# Function to load prices if they exist in blob

for file in dbutils.fs.ls(esgm_path):
  if 'assessments' in file.name:
    ass_file_path = f'/dbfs{esgm_path}/{file.name}'
  elif 'indices' in file.name:
    ind_file_path = f'/dbfs{esgm_path}/{file.name}'

# COMMAND ----------

with open(ass_file_path, 'r') as ass_file:
  ass_soup = BeautifulSoup(ass_file.read(), 'lxml-xml')
  ass_file.close()

with open(ind_file_path, 'r') as ind_file:
  ind_soup = BeautifulSoup(ind_file.read(), 'lxml-xml')
  ind_file.close()

print('Assessment prices:', ass_soup.find('returned-count').text)
print('Index prices:', ind_soup.find('returned-count').text)

# COMMAND ----------

def get_ass_details(xml_entry):
    price_dict = {'commodity_type': [price.find('c_series.commodity.name').text],
                     'last_update': [price.find('updated').text],
                     'pub_date': [price.find('c_series-order').text],
                     'price_mid': [price.find('mid').text],
                     'price_offer':[ price.find('assessment-high').text],
                     'price_bid': [price.find('assessment-low').text],
                     'start_date': [price.find('start-date').text],
                     'end_date': [price.find('end-date').text],
                     'period_abs': [price.find('period-label').text],
                     'period_rel': [price.find('c_series.timeframe.name').text],
                     'currency': [price.find('c_series.currency.name').text],
                     'unit': [price.find('c_series.size-unit.name').text],
                     'location_name': [price.find('c_series.location.name').text],
                     'publication_name': [price.find('c_series.publication.name').text],
                     'methodology': [price.find('c_series.methodology.name').text],
                     'quote_approach': [price.find('c_series.quote-approach.name').text],
                     'transaction_type': [price.find('c_series.transaction-type.name').text],
                     'price_id': [price.find('id').text]}
    price_df = pd.DataFrame.from_dict(price_dict)
    return price_df

# COMMAND ----------

def get_ind_details(xml_entry):
    """Method to extract the data from each xml entry into a dictionary and return the result as a dataframe"""
    price_dict = {'commodity_type': [price.find('c_series.commodity.name').text],
                     'last_update': [price.find('updated').text],
                     'pub_date': [price.find('c_series-order').text],
                     'price_mid': [price.find('mid').text],
                     'start_date': [price.find('start-date').text],
                     'end_date': [price.find('end-date').text],
                     'period_abs': [price.find('period-label').text],
                     'currency': [price.find('c_series.currency.name').text],
                     'unit': [price.find('c_series.size-unit.name').text],
                     'location_name': [price.find('c_series.location.name').text],
                     'publication_name': [price.find('c_series.publication.name').text],
                     'price_id': [price.find('id').text]}
    price_df = pd.DataFrame.from_dict(price_dict)
    return price_df

# COMMAND ----------

# Creating a dataframe that picks up all the price information from the XML documents
ass_df = pd.DataFrame()
ind_df = pd.DataFrame()

for price in ass_soup.find_all('entry'): # goes through each price entry and adds them to the dataframe
    ass_df = ass_df.append(get_ass_details(price), ignore_index=True)

for price in ind_soup.find_all('entry'): # goes through each price entry and adds them to the dataframe
    ind_df = ind_df.append(get_ind_details(price), ignore_index=True)

# COMMAND ----------

def clean_locations(icis_df):
  # Converting location names
  icis_df['location_name'].replace("Zeebrugge", "ZEE", inplace=True)
  icis_df['location_name'].replace("Gaspool", "GPL", inplace=True)
  icis_df['location_name'].replace("Germany", "GER", inplace=True)
  icis_df['location_name'].replace("Slovakia", "SLO", inplace=True)

  # Removing non-relevant locations
  keep_locations = ['AOC', 'GPL', 'NBP', 'NCG', 'PEG', 'PSV', 'SLO', 'TTF', 'VTP', 'ZEE', 'ZTP', 'THE']
  icis_df = icis_df[icis_df['location_name'].isin(keep_locations)]
  
  return icis_df

ass_df = clean_locations(ass_df)
ind_df = clean_locations(ind_df)

# COMMAND ----------

# Assessments: Determine duration and relative periods

def find_duration(input_period):
    if 'Month' in input_period: 
        duration = 'Month'
    elif 'WDNW' in input_period:
        duration = 'Week'
    else:
        duration = input_period.split(" ")[0] # Return first element before space
        duration = duration.split("-")[0] # Return first element before "-"
    return duration

def convert_rel_period(input_period):
    rel_period = input_period.replace(" ","")
    rel_period = rel_period.replace("+","0")
    rel_period = rel_period.replace("Day-Ahead", "D01")
    rel_period = rel_period.replace("Weekend", "WKD")
    rel_period = rel_period.replace("Month", "M")
    rel_period = rel_period.replace("Quarter", "Q")
    rel_period = rel_period.replace("Season", "S")
    rel_period = rel_period.replace("Gasyear", "GY")
    rel_period = rel_period.replace("Year", "Y")
    if "BOM" in rel_period: rel_period = "M00"
    if rel_period[-3]=="0": rel_period = rel_period[:-3] + rel_period[-2:] # if there are 3 digits, remove the redundant 0 prefix
    return rel_period

ass_df['period_duration'] = ass_df['period_rel'].apply(lambda x: find_duration(x)) # Categorizing duration of product
ass_df['period_rel'] = ass_df['period_rel'].apply(lambda x: convert_rel_period(x)) # Re-naming the labels for relative delivery period
ass_df['product_name'] = ass_df['quote_approach'].apply(lambda x: 'Price Assessments' if 'Assessment' in x else x)

ass_df.drop(['methodology', 'quote_approach'], axis=1, inplace=True) # Not used
ass_df.rename(columns={"transaction_type": "product_type"}, inplace=True)

# COMMAND ----------

# Indices: Determine product period, duration, and info

def fetch_ind_info(priceid):
    """ICIS's index information is messy and incorrect, so we extract this from the id field:
       - period_rel
       - product_name
       - transaction_type (index or spot index)
       The function returns a DataFrame with the corresponding columns that can be merged with the existing dataframe"""   
    
    # Initializing lists of words we need to use for interpreting the ID:
    keep_words = ['daily', 'monthly', 'cumulative', 'index', 'spot', 'weekend', 'day', 'within', 'ahead', 'month', 'spot']
    index_words = ['Spot', 'Index', 'Cumulative']
    
    # Extracting the relevant info from the price ID:
    index_info = []
    for ele in priceid.values:
        descriptor = [] # initializing list to keep relevant data from the string
        
        # Selecting the last part of the ID and splitting it
        id_name = ele.split('/')[-1].split('-')
        
        # Extracting the relevant words and capitalizing them
        for word in id_name:
            if (word in keep_words):
                descriptor.append(word.capitalize())
                
        # Joining the description and adding to the index_info list
        descriptor = ' '.join(descriptor)
        index_info.append(descriptor)
    
    
    # We now have a list of descriptors that we will break down into product name and index type:
    product = []
    index_type = []
    period_duration = []
    period_rel = []
    
    # Extracting info from each entry:
    for id_entry in index_info:       
        
        # Determining period duration from product name:
        if ('Month' in id_entry):
            period_duration.append('Month')
            period_rel.append('M01')
        elif ('Weekend' in id_entry):
            period_duration.append('Weekend')
            period_rel.append('WKD')
        elif ('Day' in id_entry):
            period_duration.append('Day')
            if ('Within' in id_entry):
                period_rel.append('D00') #within day
            else:
                period_rel.append('D01') #day ahead
        
        # Determining the index type:
        if ('Spot' in id_entry):
            index_type.append('Spot Index')
        elif ('Cumulative' in id_entry):
            index_type.append('Cumulative Index')
        else: # All other are per default just Index
            index_type.append('Index')
           
        # Removing index type descriptors from the product description:
        product.append(' '.join([entry for entry in id_entry.split() if entry not in index_words]))
        
    # Combining the product and index type into product name:
    product_name = []
    for entry in list(zip(product, index_type)):
        product_name.append(' '.join(entry))
    
    # Creating dataframe with all relevant information:
    price_info_df = pd.DataFrame(list(zip(product_name, period_duration, period_rel)),
                                columns=['product_name', 'period_duration', 'period_rel'])
    
    # Returning the dataframe
    return price_info_df
  
ind_info_df = fetch_ind_info(ind_df['price_id'])
ind_df['product_name'] = ind_info_df['product_name']
ind_df['period_duration'] = ind_info_df['period_duration']
ind_df['period_rel'] = ind_info_df['period_rel']

# Also add columns for bid and offer (inidces have neither)
ind_df['price_bid'] = '' # Index products do not have any values for bid nor offer
ind_df['price_offer'] = '' # Index products do not have any values for bid nor offer

ind_df['product_type'] = 'Spot'

# COMMAND ----------

# Merge dataframes
icis_df = pd.concat([ass_df, ind_df])

# COMMAND ----------

# Convert price from string to float, keep at 4 decimals (safeguard)
icis_df['price_mid'] = pd.to_numeric(icis_df['price_mid'].apply(lambda x: re.sub(',', '.', str(x)))).round(4)
icis_df['price_bid'] = pd.to_numeric(icis_df['price_bid'].apply(lambda x: re.sub(',', '.', str(x)))).round(4)
icis_df['price_offer'] = pd.to_numeric(icis_df['price_offer'].apply(lambda x: re.sub(',', '.', str(x)))).round(4)

# Converting data
icis_df['currency'] = icis_df['currency'].apply(lambda x: 'EUR' if 'Euro' in x else x)
icis_df['currency'] = icis_df['currency'].apply(lambda x: 'GBp' if 'Pence' in x else x)

# Defining and normalizing dates
icis_df['pub_date'] = icis_df['pub_date'].astype('datetime64[ns]')
icis_df['start_date'] = icis_df['start_date'].astype('datetime64[ns]')
icis_df['end_date'] = icis_df['end_date'].astype('datetime64[ns]')
icis_df['last_update'] = icis_df['last_update'].astype('datetime64[ns]')
icis_df['pub_date'] = icis_df['pub_date'].dt.normalize()
icis_df['start_date'] = icis_df['start_date'].dt.normalize()
icis_df['end_date'] = icis_df['end_date'].dt.normalize()

# Dropping BOM prices that are listed for the month after current month
icis_df.drop(icis_df.loc[(icis_df['period_abs']=='BOM') & ((icis_df['pub_date'].dt.month) < (icis_df['end_date'].dt.month))].index, axis=0, inplace=True)

icis_df['data_source'] = v_datasource_name # Defining what is the data source
icis_df.rename(columns={"price_id": "ext_price_id"}, inplace=True)

# COMMAND ----------

# Create unique key
icis_df['price_uk'] = (icis_df['publication_name'] + '/'
                       + icis_df['commodity_type'] + '/'
                       + icis_df['product_type'] + '/'
                       + icis_df['product_name'] + '/'
                       + icis_df['location_name'] + '/'
                       + icis_df['pub_date'].astype(str) + '/'
                       + icis_df['period_duration'] + '/'
                       + icis_df['end_date'].astype(str)  + '/'
                       + icis_df['currency'] + '/'
                       + icis_df['unit'])

# COMMAND ----------

# DO MORE QA ON DUPLICATED ROWS TO FIND WHAT IS THE DUPLICATED VALUE
#icis_df.loc[(icis_df.duplicated(subset='price_uk', keep=False))]

# COMMAND ----------

# Removing duplicate entries; may occur when ICIS have many ext_price_IDs for same price
icis_df.drop_duplicates(subset='price_uk', keep='first', inplace=True)

# Re-ordering columns
icis_df = icis_df[['price_uk', 'publication_name', 'data_source', 'commodity_type', 'product_type', 
                   'product_name', 'location_name', 'pub_date', 'period_rel', 'period_abs', 
                   'period_duration', 'start_date', 'end_date', 
                   'price_bid', 'price_mid', 'price_offer', 
                   'currency', 'unit', 'last_update', 'ext_price_id']]

# Resetting index
icis_df.reset_index(drop=True, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 2: Save prices to silver Delta Lake

# COMMAND ----------

# Convert to Spark DataFrame & add ingestion date
icis_spark_df = spark.createDataFrame(icis_df)
icis_spark_df = add_ingestion_date(icis_spark_df)

# COMMAND ----------

# Write to Delta table
icis_spark_df.write.format("delta").mode("overwrite").saveAsTable("prices_processed.icis_esgm_prices")

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM prices_processed.icis_esgm_prices
# MAGIC LIMIT 10

# COMMAND ----------


