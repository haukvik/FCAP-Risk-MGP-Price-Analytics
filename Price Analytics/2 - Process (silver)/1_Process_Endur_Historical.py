# Databricks notebook source
# MAGIC %md
# MAGIC # Process Endur Prices
# MAGIC 
# MAGIC Clean, transform, and refine Endur prices. Store as Delta table.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# Defining input parameters
dbutils.widgets.text("p_datasource_name", "Endur Historical")
v_datasource_name = dbutils.widgets.get("p_datasource_name")

dbutils.widgets.text("p_ingest_date", "")
v_ingest_date = dbutils.widgets.get("p_ingest_date")

# COMMAND ----------

# MAGIC %run "../0 - Includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - Includes/common_functions"

# COMMAND ----------

# Assign folder processing
process_path = f'{bronze_folder_path}/{v_ingest_date}/{v_datasource_name}'

# COMMAND ----------

# Data management imports:
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import calendar
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Endur prices

# COMMAND ----------

process_DF = spark.read.parquet(process_path)

# COMMAND ----------

# Convert Spark DF to Pandas df
endur_df = process_DF.toPandas()
endur_df['pub_date'] = pd.to_datetime(endur_df['pub_date'])
endur_df['end_date'] = pd.to_datetime(endur_df['end_date'])

# COMMAND ----------

# MAGIC %md
# MAGIC First, let's define irrelevant price curves and remove them.

# COMMAND ----------

# REMOVE CURVES WHICH ARE NOT QUOTED PRICES
start_size = endur_df.shape[0]
remove_indices = ['Q1', 'Q2', 'Q3', 'Q4', 'avg', 'av', 'bid', 'offer', 'reu', 'ecb', '4Q', 
                  '3Q', '2Q', '1Q', 'legacy', 'zzz', 'r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7', 
                  '2sah', 'off', 's1', 's2', 's3', 's4', 'sum', 'win', 'xng', 'hols', 
                  'spread', 'daily']
for ele in remove_indices:
    endur_df.drop(endur_df.loc[endur_df['index_name'].str.contains(ele, case=False)].index, axis=0, inplace=True)
end_size = endur_df.shape[0]
print('   -> Irrelevant curves removed:', start_size - end_size)

# COMMAND ----------

# Rename columns
endur_df.rename(columns={"commodity": "commodity_type", "publication": "publication_name", "rel_period": "period_rel", "price": "price_mid"}, inplace=True)

# COMMAND ----------

# Remove legacy rows
endur_df.drop(endur_df.loc[endur_df['publication_name'].str.contains('legacy', case=False)].index, axis=0, inplace=True) # remove legacy prices

# COMMAND ----------

def cast_period(rel_period):
    """Method to determine the duration of an Endur's price product"""
    if rel_period.startswith('M'): return 'Month'
    elif rel_period.startswith('Q'): return 'Quarter'
    elif rel_period.startswith('S'): return 'Season'
    elif rel_period.startswith('Y'): return 'Year'
    elif rel_period.startswith('G'): return 'Gasyear'
    elif rel_period.startswith('WKD'): return 'Weekend'
    elif rel_period.startswith('D'): return 'Day'
    else: return rel_period

# COMMAND ----------

def determine_location(index_name):
    """Procedure for extracting a location from a price curve index name"""
    loc_name = index_name.split("_")[2] # splitting the index and selecting the 3rd element (hub name)
    loc_name = loc_name.split(".")[0] # selecting the element before any "."
    if len(loc_name) > 3: # The full name of the hub is listed
        if loc_name.upper() == 'GASPOOL':
            loc_name = 'GPL'
        elif loc_name.upper() == 'ZEEHUB':
            loc_name = 'ZEE'
        elif 'PEG' in loc_name.upper():
            loc_name = 'PEG'
    if loc_name.upper() == 'ZBH':
        loc_name = 'ZEE'
    return loc_name

# COMMAND ----------

def determine_period(pub_name, index_name):
    """Procedure to determine and return the period duration of the given price product"""
    
    # Remove non-standard symbols from input:
    pub = re.sub('(_|\+|\-|\.)', ' ', pub_name.lower())
    index = re.sub('(_|\+|\-|\.)', ' ', index_name.lower())
    #print(pub)
    
    # Create a list of publication for comparison of all elements
    pub_list = re.split(' ', pub) # splitting by spaces " " and underscores "_"
    index_list = re.split(' ', index)
    #print(pub_list)
    
    day_list = ['daily', 'dah', 'day', 'days', 'da', 'eod', 'egsi']
    weekend_list = ['weekend', 'wsi', 'wi', 'wkd', 'we', 'wknd']
    week_list = ['bow', 'wdnw']
    month_list = ['mah', 'month', 'months', 'fm', 'monthly']
    quarter_list = ['quarter', 'quarters', 'qah', 'q1', 'q2', 'q3', 'q4']
    season_list = ['season', 'seasons', 'sah', 'summer', 'winter']
    gasyear_list = ['gah', 'gyah', 'gasyear', 'gy', 'yearsgas', 'gasyears']
    calyear_list = ['cal', 'calyear', 'year', 'years', 'yah', 'calyears']
    
    if ('heren fin' in pub):
        # This can be anything since it's a general purpose name
        if bool(set(index_list) & set(week_list)):
            return 'Week'
        elif bool(set(index_list) & set(month_list)):
            return 'Month'
        elif bool(set(index_list) & set(quarter_list)):
            return 'Quarter'
        elif bool(set(index_list) & set(season_list)):
            return 'Season'
        elif bool(set(index_list) & set(gasyear_list)):
            return 'Gasyear'
        elif bool(set(index_list) & set(calyear_list)):
            return 'Year'
        else:
            return 'Day'
    elif bool(set(pub_list) & set(week_list)):
        return 'Week'
    elif bool(set(pub_list) & set(month_list)):
        return 'Month'
    elif bool(set(pub_list) & set(quarter_list)):
        return 'Quarter'
    elif bool(set(pub_list) & set(season_list)):
        return 'Season'
    elif bool(set(pub_list) & set(gasyear_list)):
        return 'Gasyear'
    elif bool(set(pub_list) & set(calyear_list)):
        return 'Year'
    elif bool(set(pub_list) & set(day_list)):
        return 'Day'
    elif bool(set(pub_list) & set(weekend_list)):
        return 'Weekend'
    else:
        return 'NA'

# COMMAND ----------

# ZERO PRICES: We keep them for now - fix root cause in Endur later

# Show them
#zeros_df = endur_df.loc[endur_df['price_mid']==0]
#zeros_df.loc[zeros_df['pub_date']=='2021-10-07']

# If we want to remove them:
#endur_df.drop(endur_df.loc[endur_df['price_mid']==0].index, axis=0, inplace=True)

# COMMAND ----------

# Determine the duration of a given price entry:

# Create a new temporary dataframe
period_df = pd.DataFrame(columns=['index', 'period_duration'])

# For each row, identify period duration
for index, row in endur_df.iterrows():
    pub_name = row[2]
    index_name = row[1]
    period_duration = determine_period(pub_name, index_name)
    period_df = period_df.append({'index': index, 'period_duration': period_duration}, ignore_index=True)
period_df.set_index('index', inplace=True)

# Merging the period into the main dataframe
endur_df = pd.concat([endur_df, period_df], axis=1, sort=False)

# Removing NA entries; these are not used for anything
endur_df.drop(endur_df.loc[endur_df['period_duration']=='NA'].index, axis=0, inplace=True)

# COMMAND ----------

print(' - Cleaning and processing Endur prices...')

# REMOVING, RENAMING, FORMATTING
endur_df['commodity_type'].replace("NatGas", "Natural Gas", inplace=True)

# Setting default values
endur_df['data_source'] = 'Endur Historical' # All prices from this source are this type
endur_df['start_date'] = np.nan # Endur has no information about this
endur_df['start_date'] = endur_df['start_date'].astype('datetime64[ns]')

endur_df['price_bid'] = np.nan # only mid prices as default
endur_df['price_offer'] = np.nan
endur_df['ext_price_id'] = endur_df['index_name'].copy(deep=True) # using index name as external system id

endur_df['period_rel'] = '' # To be added later if necessary

# Converting Endur's currency use of GBP to GBp
endur_df['currency'] = endur_df['currency'].apply(lambda x: 'GBp' if 'GBP' in x else x) # changing currency name
endur_df['price_mid'] = np.where(endur_df['currency'] == 'GBp', endur_df['price_mid']*100, endur_df['price_mid']) # GBp / 100

# Determine the price location based on curve index name
endur_df['location_name'] = endur_df['index_name'].apply(lambda x: determine_location(x))

# Ensure delivery unit follows naming convention when 'therm'
endur_df['unit'] = endur_df['unit'].apply(lambda x: 'therm' if 'therm' in x.lower() else x)

# COMMAND ----------

def generate_publication_info(pub_name, index_name):
    """Procedure to interpret the publication field for information. Returns: Publication name, product name & product type."""
    if any(ele in pub_name.upper() for ele in ['PNX', 'EEX', 'POWERNEXT']):
        publication_name = 'EEX'
        if 'EOD' in pub_name.upper():
            product_name = 'EOD'
            product_type = 'Spot'
        elif 'EGSI' in pub_name.upper():
            product_name = 'EGSI'
            product_type = 'Spot'
        else:
            product_name = 'Settlement'
            product_type = 'Futures'
    elif ('HEREN' in pub_name.upper()) or ('ESGM' in index_name.upper()):
        publication_name = 'European Spot Gas Markets' # Endur only contains ESGM publications
        product_type = 'Spot' # All ESGM publications are tagged as 'Spot'
        
        # IF the publication in Endur does not denote it's an index, it's an assessment
        if any(ele in index_name.upper() for ele in ['_IND_']): # ICIS indices
            if ('MONTHCUM' in index_name.upper()): # Cumulative index is denoted such
                product_name = 'Monthly Cumulative Index'
            elif ('D_MAH' in index_name.upper()):
                product_name = 'Daily Month Ahead Index'
            elif ('MONTHLY' in index_name.upper()):
                product_name = 'Monthly Index'
            elif ('DA+WSI' in index_name.upper()):
                product_name = 'Day Ahead Index and Weekend Spot Index'
            elif ('DA+WI' in index_name.upper()):
                product_name = 'Day Ahead Index and Weekend Index'
            else:
                product_name = 'Non-identified Index'
        else: # The product is not index, i.e. it is Assessment
            if ('CONT' in index_name.upper()):
                product_name = 'Continental Price Assessments'
            else:
                product_name = 'Price Assessments'
        
    #return publication_name, product_name, product_type
    return {'publication_name': [publication_name], 'product_name': [product_name], 'product_type': [product_type]}

# COMMAND ----------

# Go through each row and identify publication_name, product_name, and product_type

for index, row in endur_df.loc[:,['publication_name', 'index_name']].iterrows():
    pub_info = generate_publication_info(row['publication_name'], row['index_name'])
    endur_df.loc[endur_df.index == index, 'publication_name'] = pub_info['publication_name']
    endur_df.loc[endur_df.index == index, 'product_name'] = pub_info['product_name']
    endur_df.loc[endur_df.index == index, 'product_type'] = pub_info['product_type']

# Drop the index_name column; no longer used
endur_df = endur_df.drop(['index_name'], axis=1)

# COMMAND ----------

#endur_df.loc[(endur_df['pub_date']=='2021-10-07') & (endur_df['location_name']=='PEG') & (endur_df['publication_name']!='EEX')].sort_values(by=['publication_name', 'period_duration', 'end_date'])

# COMMAND ----------

# Correcting and removing invalid dates

# A - If the MAH index references end date = pub date, fix it
correction_df = endur_df.loc[(endur_df['product_name'].str.contains('Daily Month Ahead Index')) &
                          (endur_df['pub_date']==endur_df['end_date'])].copy(deep=True)

# COMMAND ----------

#correction_df.head(2)

# COMMAND ----------

def last_date_next_month(input_date):

    import datetime
    import calendar

    #input_date = '2020-12-01'

    # Storing given month and year
    year_dt, month_dt = input_date.year, input_date.month
    
    # Determining next month and year
    if (month_dt == 12):
        year_dt += 1
        month_dt = 1
    else:
        month_dt += 1

    # Last day of next month:
    last_day = calendar.monthrange(year_dt, month_dt)[1]

    # Creating the full correct date
    correct_date_dt = datetime.datetime(year_dt, month_dt, last_day)
    correct_date = datetime.datetime.strftime(correct_date_dt, '%Y-%m-%d')
    return correct_date

# COMMAND ----------

#correction_df.dtypes

# COMMAND ----------

# Correcting the end date for the Monthly indices
correction_df['end_date'] = correction_df['pub_date'].apply(lambda x: last_date_next_month(x.date()))
correction_df['end_date'] = correction_df['end_date'].astype('datetime64[ns]')

# Updating the Endur dataframe with correct end dates:
endur_df.update(correction_df)

# COMMAND ----------

# Remove entries with invalid publication/end date combinations
# NB: Only within-day products are quoted on the same day as published
remove_index = endur_df[(endur_df['pub_date'] == endur_df['end_date']) & 
                        (endur_df['period_duration'] != 'Day')].index
endur_df.drop(axis=0, index=remove_index, inplace=True)

# COMMAND ----------

# Sorting the data set:
endur_df = endur_df.sort_values(['pub_date', 'location_name', 'publication_name', 'end_date', 'product_type', 'product_name'], ascending=[True, True, True, True, False, True])

# Creating a unique key per product to identify duplicates
endur_df['price_uk'] = (endur_df['publication_name'] + '/' 
                        + endur_df['commodity_type'] + '/'
                        + endur_df['product_type'] + '/' 
                        + endur_df['product_name'] + '/' 
                        + endur_df['location_name'] + '/' 
                        + endur_df['pub_date'].astype(str) + '/' 
                        + endur_df['period_duration'].astype(str) + '/' 
                        + endur_df['end_date'].astype(str)  + '/'
                        + endur_df['currency'] + '/'
                        + endur_df['unit'])

endur_df['period_abs'] = ''

# Re-ordering columns
endur_df = endur_df[['price_uk',
                     'publication_name', 
                     'data_source', 
                     'commodity_type', 
                     'product_type', 
                     'product_name', 
                     'location_name', 
                     'pub_date', 
                     'period_rel', 
                     'period_abs', 
                     'period_duration', 
                     'start_date', 
                     'end_date', 
                     'price_bid', 
                     'price_mid', 
                     'price_offer', 
                     'currency', 
                     'unit', 
                     'last_update', 
                     'ext_price_id']]

# COMMAND ----------

def inconsistent_prices(input_df):
    """Procedure to identify inconsistent prices internally in Endur"""
    # Extracting duplicates
    input_df = input_df[input_df['price_uk'].duplicated(keep=False)]
    input_df = input_df.sort_values(['price_uk'], ascending=['True'])

    # Grouping Endur price products that are identical per uniqe key
    group_df = input_df.groupby(['price_uk'])['price_mid']\
          .agg([min, max, 'count'])

    # Identifying mismatching prices
    price_mismatch_df = group_df[group_df['min']!=group_df['max']]
    
    inconsistent_df = input_df[input_df['price_uk'].isin(price_mismatch_df.index)]\
                                 .groupby(['price_uk', 'ext_price_id', 'product_name', 'pub_date', 'end_date', 'price_mid'])\
                                 ['price_mid'].count()
    
    # Retrieve curves which are inconsistent
    inconsistent_curves = input_df.loc[input_df['price_uk'].isin(price_mismatch_df.index.values)]['ext_price_id']
    
    return inconsistent_df, inconsistent_curves

# COMMAND ----------

# Optional: Show inconsistent prices in Endur
incost_df, incost_curves = inconsistent_prices(endur_df)

if len(incost_df)>0:
  print('NB! Endur might contain inconsistent prices (duplicate curves with different prices). These are displayed below.')
  print(incost_df)

# COMMAND ----------

# Dropping legacy curves from above list (keeping only the most relevant)
drop_filter = ['MAH', 'SAH', 'QAH', 'YAH']
drop_curves = []
for curve in incost_curves:
  for filter in drop_filter:
    if filter in curve:
      drop_curves.append(curve)

# Drop curves given filter conditions
endur_df.drop(index=endur_df.loc[endur_df['ext_price_id'].isin(drop_curves)].index, inplace=True)

# COMMAND ----------

# NOTE: ASSESS LOGIC FOR IMPROVING CURVE SELECTION IN BELOW DUPLICATE REMOVAL

# create list of unique keys with duplicates
dupe_uk = endur_df.loc[endur_df['price_uk'].duplicated()]['price_uk'].values

# Remove duplicate rows if they exist; Endur saves prices very redundantly
endur_df.drop_duplicates(subset='price_uk', keep='first', inplace=True)

# Resetting index to be 'price_uk'
endur_df.reset_index(drop=True, inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save prices to silver Delta lake

# COMMAND ----------

# Convert to Spark DataFrame
endur_spark_df = spark.createDataFrame(endur_df)

# COMMAND ----------

# Add ingestion timestamp
endur_spark_df = add_ingestion_date(endur_spark_df)

# COMMAND ----------

# Write to Delta table
endur_spark_df.write.format("delta").mode("overwrite").saveAsTable("prices_processed.endur_prices")

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")
