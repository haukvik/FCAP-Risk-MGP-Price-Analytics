# Databricks notebook source
# MAGIC %md
# MAGIC # Price Comparison
# MAGIC ---
# MAGIC This part of the process compares the prices of the published indices.  
# MAGIC 
# MAGIC For now, that means Endur vs the Price Reporting Agency.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# Defining input parameters
dbutils.widgets.text("p_compare_from_date", "2021-10-14")
v_compare_from_date = dbutils.widgets.get("p_compare_from_date")

dbutils.widgets.text("p_compare_to_date", "2021-10-14")
v_compare_to_date = dbutils.widgets.get("p_compare_to_date")

# COMMAND ----------

# MAGIC %run "../0 - Includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - Includes/common_functions"

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load prices from processed tables

# COMMAND ----------

# COMBINE SILVER TABLES & CREATE VIEW

ngprices_DF = spark.sql('SELECT * \
                          FROM prices_processed.endur_prices \
                          UNION SELECT * FROM prices_processed.eex_prices \
                          UNION SELECT * FROM prices_processed.icis_esgm_prices')

ngprices_DF.createOrReplaceTempView('ngprices')

# COMMAND ----------

# SELECTION CRITERIA
#datefilter = ' AND a.pub_date BETWEEN \'' +v_compare_from_date+ '\' AND \'' +v_compare_to_date+ '\''
order = ' ORDER BY a.pub_date, a.end_date, a.location_name, a.product_name ASC'

# COMMAND ----------

# Query to match prices between Endur and External sources (excluding weekend)
# The weekends will be excluded from this query since Endur does not denote weekends
recon_query = """
        SELECT a.price_uk ext_price_uk, b.price_uk endur_price_uk, a.pub_date, a.commodity_type, a.product_type, a.publication_name, a.product_name, a.location_name, a.period_duration, a.end_date,
        a.price_mid ext_price_mid, b.price_mid endur_price_mid,
        a.data_source ds_ext, b.data_source ds_int, b.ext_price_id endur_curve
        FROM ngprices a
        INNER JOIN ngprices b
        ON a.publication_name = b.publication_name
        AND a.pub_date = b.pub_date
        AND a.commodity_type = b.commodity_type
        AND a.product_type = b.product_type
        AND a.product_name = b.product_name
        AND a.location_name = b.location_name
        AND a.period_duration = b.period_duration
        AND a.end_date = b.end_date
        AND a.unit = b.unit
        AND a.currency = b.currency
        AND a.data_source NOT LIKE b.data_source
        WHERE LOWER(a.data_source) NOT LIKE '%endur%'
        AND LOWER(a.period_duration) NOT LIKE 'weekend'
        """ + order

# COMMAND ----------

# Defining separate query to compare Endur and External weekend prices
# In Endur, weekends are tagged as "Day" durations
recon_wkd_query = """
        SELECT a.price_uk ext_price_uk, b.price_uk endur_price_uk, a.pub_date, a.commodity_type, a.product_type, a.publication_name, a.product_name, a.location_name, a.period_duration, a.end_date,
        a.price_mid ext_price_mid, b.price_mid endur_price_mid,
        a.data_source ds_ext, b.data_source ds_int, b.ext_price_id endur_curve
        FROM ngprices a
        INNER JOIN ngprices b
        ON a.publication_name = b.publication_name
        AND a.pub_date = b.pub_date
        AND a.commodity_type = b.commodity_type
        AND a.product_type = b.product_type
        AND a.product_name = b.product_name
        AND a.location_name = b.location_name
        AND a.end_date = b.end_date
        AND a.unit = b.unit
        AND a.currency = b.currency
        AND a.data_source NOT LIKE b.data_source
        AND (LOWER(a.period_duration) LIKE 'weekend' AND LOWER(b.period_duration) LIKE 'day')
        WHERE LOWER(a.data_source) NOT LIKE '%endur%'
        """ + order

# COMMAND ----------

# Query to match ICIS Indicies with Endur prices: MONTHLY
# Separate query required since Endur combines DA with WI/WSI as product name
recon_icis_ind_query = """
        SELECT a.price_uk ext_price_uk, b.price_uk endur_price_uk, a.pub_date, a.commodity_type, a.product_type, a.publication_name, a.product_name, a.location_name, a.period_duration, a.end_date,
        a.price_mid ext_price_mid, b.price_mid endur_price_mid,
        a.data_source ds_ext, b.data_source ds_int, b.ext_price_id endur_curve
        FROM ngprices a
        INNER JOIN ngprices b
        ON a.publication_name = b.publication_name
        AND a.pub_date = b.pub_date
        AND a.commodity_type = b.commodity_type
        AND a.product_type = b.product_type
        AND a.location_name = b.location_name
        AND a.end_date = b.end_date
        AND a.unit = b.unit
        AND a.currency = b.currency
        AND a.data_source NOT LIKE b.data_source
        AND a.product_name = b.product_name
        AND LOWER(a.product_name) LIKE '%index%'
        WHERE LOWER(a.data_source) NOT LIKE '%endur%'
        """ + order
#prices_icis_ind_df = pd.read_sql(recon_icis_ind_query, ng_prices, parse_dates=['pub_date', 'start_date', 'end_date', 'last_update'])

# COMMAND ----------

# Executing above queries and assigning to dataframes
prices_df = spark.sql(recon_query).toPandas()
prices_wkd_df = spark.sql(recon_wkd_query).toPandas()
prices_icis_ind_df = spark.sql(recon_icis_ind_query).toPandas()

print('Standard prices:', prices_df.shape[0])
print('Weekend prices:', prices_wkd_df.shape[0])
print('ICIS Index prices:', prices_icis_ind_df.shape[0])

# COMMAND ----------

# Merging the Weekend comparisons into the main dataframe:
prices_df = pd.concat([prices_df, prices_wkd_df, prices_icis_ind_df])
prices_df.reset_index(drop=True, inplace=True) # resetting the row indexation

# COMMAND ----------

# Calculate the absolute differences in mid prices
prices_df['price_mid_diff_abs'] = round(abs(prices_df['ext_price_mid']-prices_df['endur_price_mid']), 10)

# COMMAND ----------

# Re-ordering columns
prices_df = prices_df[['pub_date'
                      ,'publication_name'
                      ,'commodity_type'
                      ,'product_type'
                      ,'product_name'
                      ,'location_name'
                      ,'period_duration'
                      ,'end_date'
                      ,'endur_price_mid'
                      ,'ext_price_mid'
                      ,'price_mid_diff_abs'
                      ,'endur_curve'
                      ,'ds_ext'
                      ,'ds_int'
                      ,'ext_price_uk'
                      ,'endur_price_uk']]

# Sorting based on first columns
prices_df = prices_df.sort_values(by=prices_df.columns[0:8].values.tolist(), ascending=True)

# COMMAND ----------

# Comparing prices and tagging as match or mismatch, depending on threshold value
mismatch_thresh = 0.0005
prices_df['price_comparison'] = 'Match' # default value
prices_df['price_comparison'] = prices_df['price_mid_diff_abs'].apply(lambda x: 'Mismatch' if x > mismatch_thresh else 'Match')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reporting result

# COMMAND ----------

no_comparisons = len(prices_df)
no_mismatches = len(prices_df.loc[prices_df['price_comparison'] == 'Mismatch'])

print('\nNumber of prices compared between ETRM (Endur) and external sources:', str(no_comparisons))
print('Number of mismatching prices between ETRM (Endur) and external sources:', str(no_mismatches))

if (no_mismatches>0):
    print('Below are the mismatching prices:\n')

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Save Result to exposed/gold Delta Table

# COMMAND ----------

# Convert to Spark DataFrame
reconciled_prices_df = spark.createDataFrame(prices_df)

# COMMAND ----------

# Write to Delta table
reconciled_prices_df.write.format("delta").mode("overwrite").saveAsTable("prices_exposed.reconciled_prices")

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM prices_exposed.reconciled_prices;
