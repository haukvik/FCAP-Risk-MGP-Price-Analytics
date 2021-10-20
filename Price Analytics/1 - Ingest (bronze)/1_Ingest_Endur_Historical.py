# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Prices from Endur Historical tables
# MAGIC 
# MAGIC Fetch and store as parquet in bronze layer.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# Defining input parameters
dbutils.widgets.text("p_datasource_name", "")
v_datasource_name = dbutils.widgets.get("p_datasource_name")

dbutils.widgets.text("p_ingest_date", "")
v_ingest_date = dbutils.widgets.get("p_ingest_date")

dbutils.widgets.text("p_start_date", "")
v_start_date = dbutils.widgets.get("p_start_date")

dbutils.widgets.text("p_end_date", "")
v_end_date = dbutils.widgets.get("p_end_date")

# COMMAND ----------

# MAGIC %run "../0 - Includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - Includes/common_functions"

# COMMAND ----------

# Create folder for ingestion
ingest_path = f'{bronze_folder_path}/{v_ingest_date}/{v_datasource_name}'
dbutils.fs.mkdirs(f'{ingest_path}/000')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fetch relevant prices from Omnia Endur's Staging Area

# COMMAND ----------

# Data management imports:
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import calendar
import re

# COMMAND ----------

# SELECTION CRITERIA
datefilter = ' AND reset_date BETWEEN \'' +v_start_date+ '\' AND \'' +v_end_date+ '\''
curvefilter = "AND (LOWER(index_name) LIKE \'%pnx%\' OR LOWER(index_name) LIKE \'%esgm%\')"

# COMMAND ----------

# Create query statement to select the latest price from Endur's historical price tables
query = """SELECT comm.name commodity, idx.index_name, ref.name publication, hist.reset_date pub_date,  
            hist.start_date end_date, hist.price, ccy.name currency, unit.unit_label unit, hist.last_update
            FROM ((((([app_market_risk].[view_vw_idx_def] idx
            INNER JOIN [app_market_risk].[view_vw_currency] ccy ON idx.currency = ccy.id_number)
            INNER JOIN [app_market_risk].[view_vw_idx_unit] unit ON idx.unit = unit.unit_id)
            INNER JOIN [app_market_risk].[view_vw_idx_historical_prices] hist ON idx.index_id = hist.index_id)
            INNER JOIN [app_market_risk].[view_vw_ref_source] ref ON hist.ref_source = ref.id_number)
            INNER JOIN [app_market_risk].[view_vw_delivery_type] comm ON idx.delivery_type = comm.id_number)
            WHERE idx.db_status=1 AND idx.index_version_id = (SELECT MAX(index_version_id) FROM [app_market_risk].[view_vw_idx_def] idx2 WHERE idx2.index_id = idx.index_id)
            """ + curvefilter + datefilter
#print(query)

# COMMAND ----------

# Connect to Endur, load query results to dataframe
print(' - Connecting to mssgtdtrprod.database.windows.net and retrieving Historical Prices from', v_start_date, 'to', v_end_date)
endur_DF = queryEndur(query)
print(' - Loaded a total of', endur_DF.count(), 'prices.')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Bronze as parquet

# COMMAND ----------

endur_DF.write.parquet(path=ingest_path, mode='overwrite')

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")
