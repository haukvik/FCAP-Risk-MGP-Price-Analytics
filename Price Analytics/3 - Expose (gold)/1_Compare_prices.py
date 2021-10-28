# Databricks notebook source
# MAGIC %md
# MAGIC # Price Comparison
# MAGIC ---
# MAGIC This part of the process compares the trading system (Endur) prices vs the published indices (external sources).  
# MAGIC 
# MAGIC 
# MAGIC ---

# COMMAND ----------

# Defining input parameters
dbutils.widgets.text("p_compare_from_date", "")
v_compare_from_date = dbutils.widgets.get("p_compare_from_date")

dbutils.widgets.text("p_compare_to_date", "")
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

ngprices_DF = spark.sql(f"SELECT * \
                          FROM prices_processed.endur_prices \
                          UNION SELECT * FROM prices_processed.eex_prices \
                          UNION SELECT * FROM prices_processed.icis_esgm_prices")

ngprices_DF.createOrReplaceTempView('ngprices')

# COMMAND ----------

# We define a threshold value to identify mismatches
mismatch_threshold = 0.00051 # this is typically the threshold for rounding differences
mismatch_decimals = 6

# COMMAND ----------

# General query to compare Endur prices vs other sources
compare_general_sql = f"""
  SELECT DATE(endur.pub_date)
        ,endur.publication_name
        ,endur.commodity_type
        ,endur.product_type
        ,endur.product_name
        ,endur.location_name
        ,endur.period_duration
        ,DATE(endur.end_date) price_end_date
        ,ROUND(endur.price_mid,6) endur_mid_price
        ,ext.price_mid ext_mid_price
        ,ROUND(ABS(endur.price_mid - ext.price_mid),5) mid_price_diff
        ,CASE 
          WHEN ROUND(ABS(endur.price_mid - ext.price_mid),{mismatch_decimals}) = 0 THEN 'True'
          WHEN ROUND(ABS(endur.price_mid - ext.price_mid),{mismatch_decimals}) <= {mismatch_threshold} THEN 'True (rounding diff)'
          ELSE 'False'
          END AS price_match
        ,endur.src_price_id endur_curve
        ,endur.unit
        ,endur.currency
        ,endur.price_uk endur_price_uk
        ,ext.price_uk ext_price_uk
  FROM ngprices endur
  INNER JOIN ngprices ext
    ON endur.price_uk = ext.price_uk
  WHERE LOWER(endur.data_source) LIKE '%endur%'
    AND LOWER(ext.data_source) NOT LIKE '%endur%'
    --AND endur.pub_date BETWEEN '{v_compare_from_date}' AND '{v_compare_to_date}'
  ORDER BY pub_date, publication_name, product_type, product_name, location_name, price_end_date, period_duration
"""
compare_general_DF = spark.sql(compare_general_sql)

# COMMAND ----------

# Query for weekend products separately since logic differs
compare_weekend_sql = f"""
  SELECT DATE(endur.pub_date)
        ,endur.publication_name
        ,endur.commodity_type
        ,endur.product_type
        ,endur.product_name
        ,endur.location_name
        ,endur.period_duration
        ,DATE(endur.end_date) price_end_date
        ,ROUND(endur.price_mid,6) endur_mid_price
        ,ext.price_mid ext_mid_price
        ,ROUND(ABS(endur.price_mid - ext.price_mid),6) mid_price_diff
        ,CASE 
          WHEN ROUND(ABS(endur.price_mid - ext.price_mid),{mismatch_decimals}) = 0 THEN 'True'
          WHEN ROUND(ABS(endur.price_mid - ext.price_mid),{mismatch_decimals}) <= {mismatch_threshold} THEN 'True (rounding diff)'
          ELSE 'False'
          END AS price_match
        ,endur.src_price_id endur_curve
        ,endur.unit
        ,endur.currency
        ,endur.price_uk endur_price_uk
        ,ext.price_uk ext_price_uk
  FROM ngprices endur
  INNER JOIN ngprices ext -- join on almost everything except duration
    ON endur.pub_date = ext.pub_date
    AND endur.publication_name = ext.publication_name
    AND endur.commodity_type = ext.commodity_type
    AND endur.product_type = ext.product_type
    AND endur.product_name = ext.product_name
    AND endur.location_name = ext.location_name
    AND endur.unit = ext.unit
    AND endur.currency = ext.currency
    AND endur.end_date = ext.end_date
  WHERE LOWER(endur.data_source) LIKE '%endur%'
    AND LOWER(ext.data_source) NOT LIKE '%endur%'
    AND LOWER(endur.period_duration) IN ('day', 'weekend')
    AND LOWER(ext.period_duration)  = 'weekend'
  ORDER BY pub_date, publication_name, product_type, product_name, location_name, price_end_date, period_duration
"""
compare_weekend_DF = spark.sql(compare_weekend_sql)

# COMMAND ----------

# Query for DAY AHEAD & WEEKEND SPOT INDEX products separately since logic differs
compare_da_wsi_sql = f"""
  SELECT DATE(endur.pub_date)
        ,endur.publication_name
        ,endur.commodity_type
        ,endur.product_type
        ,endur.product_name
        ,endur.location_name
        ,endur.period_duration
        ,DATE(endur.end_date) price_end_date
        ,ROUND(endur.price_mid,6) endur_mid_price
        ,ext.price_mid ext_mid_price
        ,ROUND(ABS(endur.price_mid - ext.price_mid),6) mid_price_diff
        ,CASE 
          WHEN ROUND(ABS(endur.price_mid - ext.price_mid),{mismatch_decimals}) = 0 THEN 'True'
          WHEN ROUND(ABS(endur.price_mid - ext.price_mid),{mismatch_decimals}) <= {mismatch_threshold} THEN 'True (rounding diff)'
          ELSE 'False'
          END AS price_match
        ,endur.src_price_id endur_curve
        ,endur.unit
        ,endur.currency
        ,endur.price_uk endur_price_uk
        ,ext.price_uk ext_price_uk
  FROM ngprices endur
  INNER JOIN ngprices ext -- join on almost everything except duration
    ON endur.pub_date = ext.pub_date
    AND endur.publication_name = ext.publication_name
    AND endur.commodity_type = ext.commodity_type
    AND endur.product_type = ext.product_type
    AND endur.location_name = ext.location_name
    AND endur.unit = ext.unit
    AND endur.currency = ext.currency
    AND endur.end_date = ext.end_date
  WHERE LOWER(endur.data_source) LIKE '%endur%'
    AND LOWER(ext.data_source) NOT LIKE '%endur%'
    AND LOWER(endur.product_name) = 'day ahead index and weekend spot index'
    AND LOWER(ext.product_name) IN ('day ahead index', 'weekend spot index')
  ORDER BY pub_date, publication_name, product_type, product_name, location_name, price_end_date, period_duration
"""
compare_da_wsi_DF = spark.sql(compare_da_wsi_sql)

# COMMAND ----------

# combining dataframes
full_comparison_DF = compare_general_DF.union(compare_weekend_DF).union(compare_da_wsi_DF)

# removing potential duplicates
publish_comparison_DF = full_comparison_DF.drop_duplicates()

# COMMAND ----------

# print('Total number of compared prices:', full_comparison_DF.count())
# print('Compared standard prices:', compare_general_DF.count())
# print('Compared weekend prices:', compare_weekend_DF.count())
# print('Compared ICIS ESGM DA+WSI prices:', compare_da_wsi_DF.count())

# COMMAND ----------

# print('Fully matching prices:', publish_comparison_DF.filter(publish_comparison_DF.price_match == 'True').count())
# print('Matching prices with rounding difference:', publish_comparison_DF.filter(publish_comparison_DF.price_match == 'True (rounding diff)').count())
# print('Mismatching prices:', publish_comparison_DF.filter(publish_comparison_DF.price_match == 'False').count())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Save Result to exposed/gold Delta Table

# COMMAND ----------

# Add comparison timestamp
from pyspark.sql.functions import current_timestamp
publish_comparison_DF = publish_comparison_DF.withColumn('comparison_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save comparison result to delta table
# MAGIC This is for operational Risk and IT to verify price correctness.

# COMMAND ----------

# Performing upsert into exposed comparison table
merge_condition = 'tgt.endur_price_uk = src.endur_price_uk AND tgt.ext_price_uk = src.ext_price_uk' # merge based on identical unique keys
merge_delta_gold(publish_comparison_DF, 'prices_exposed', 'compared_prices', gold_folder_path, merge_condition, 'pub_date')

# COMMAND ----------

# Write to parquet for PowerBI expose

# Create folder for PowerBI reporting
powerbi_path = f'{gold_folder_path}/powerbi_reports'
dbutils.fs.mkdirs(f'{powerbi_path}/000')

# PowerBI expects a given file name; converting to pandas first to enable this export criterion
powerbi_export_df = publish_comparison_DF.toPandas()
powerbi_export_df.to_parquet(path=f'/dbfs{powerbi_path}/ng_compared_prices.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save new mismatches to delta table
# MAGIC This is for management reporting. 
# MAGIC 
# MAGIC The first run every morning reports the number of prices, including possible mismatches. Any corrections during the day are not included in this report.

# COMMAND ----------

from pyspark.sql.functions import count, when, col, lit, sum, max, date_format

# COMMAND ----------

# creating the report
price_report_DF = publish_comparison_DF\
  .select(date_format(max('comparison_date'), 'yyyy-MM-dd').alias('comparison_date')\
         ,count('*').alias('no_prices_compared')\
         ,count(when(publish_comparison_DF.price_match == 'True', 1)).alias('no_price_matches')\
         ,count(when(publish_comparison_DF.price_match == 'True (rounding diff)', 1)).alias('no_price_matches_with_rounding_diff')\
         ,count(when(publish_comparison_DF.price_match == 'False', 1)).alias('no_price_mismatches')
         )

# COMMAND ----------

# defining delta merge function to merge on comparison_date, which only inserts new entries

def update_daily_price_report(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

  from delta.tables import DeltaTable
  
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")): # if table already exists, upsert merge
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition)\
      .whenNotMatchedInsertAll()\
      .execute()
  else: # if table does not exist, write directly
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# inserting new comparisons
merge_condition = 'tgt.comparison_date = src.comparison_date'
update_daily_price_report(price_report_DF, 'prices_exposed', 'daily_price_report', gold_folder_path, merge_condition, 'comparison_date')

# COMMAND ----------

# Write to parquet for PowerBI expose

# Create folder for PowerBI reporting
powerbi_path = f'{gold_folder_path}/powerbi_reports'
dbutils.fs.mkdirs(f'{powerbi_path}/000')

# PowerBI expects a given file name; converting to pandas first to enable this export criterion
powerbi_export_df = spark.read.table('prices_exposed.daily_price_report').toPandas()
powerbi_export_df.to_parquet(path=f'/dbfs{powerbi_path}/ng_daily_price_report.parquet')

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")
