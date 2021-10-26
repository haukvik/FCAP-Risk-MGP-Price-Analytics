# Databricks notebook source
# Common functions to be used in Price Reconciliation activity

# Below are common functions and schema used in the activity. These are defined centrally here to make the notebooks efficient as well as to keep things consistent.

# COMMAND ----------

# Create Spark DataFrame schema for processed prices:
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, DateType

prices_schema = StructType(fields=[StructField('price_uk', StringType(), False),
                                   StructField('publication_name', StringType(), False),
                                   StructField('data_source', StringType(), False),
                                   StructField('commodity_type', StringType(), False),
                                   StructField('product_type', StringType(), False),
                                   StructField('product_name', StringType(), False),
                                   StructField('location_name', StringType(), False),
                                   StructField('pub_date', DateType(), False),
                                   StructField('period_rel', StringType(), True),
                                   StructField('period_abs', StringType(), True),
                                   StructField('period_duration', StringType(), True),
                                   StructField('start_date', DateType(), True),
                                   StructField('end_date', DateType(), False),
                                   StructField('price_bid', FloatType(), True),
                                   StructField('price_mid', FloatType(), True),
                                   StructField('price_offer', FloatType(), True),
                                   StructField('currency', StringType(), True), # true since it can be fx
                                   StructField('unit', StringType(), True), # same as above
                                   StructField('src_last_update', TimestampType(), False),
                                   StructField('src_price_id', StringType(), True)
                                  ])

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

def merge_delta_silver(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

  from delta.tables import DeltaTable
  
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")): # if table already exists, upsert merge
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition)\
      .whenMatchedUpdateAll(condition='tgt.src_last_update < src.src_last_update')\
      .whenNotMatchedInsertAll()\
      .execute()
  else: # if table does not exist, write directly
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_gold(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

  from delta.tables import DeltaTable
  
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")): # if table already exists, upsert merge
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition)\
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else: # if table does not exist, write directly
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def queryEndur(db_query):
  # Query to execute towards the Endur data
  
  import adal
  
  sp_id = dbutils.secrets.get(scope = secret_scope_name, key = 'fcap-sp-id')
  sp_secret = dbutils.secrets.get(scope = secret_scope_name, key = 'fcap-sp-secret')
  tenant_id = dbutils.secrets.get(scope = secret_scope_name, key = 'fcap-sp-tenant-id')
  
  resource_app_id_url = 'https://database.windows.net/'
  authority = 'https://login.windows.net/' + tenant_id
  
  # Connection string to database server, and db name
  azure_sql_url = 'jdbc:sqlserver://mssgtdtrprod.database.windows.net'
  db_name = 'mss_gtdtradingrisk_prod_db'
  
  # Security settings
  encrypt = 'true'
  host_name_in_certificate = '*.database.windows.net'

  # Authenticate as Service Principal and get access token
  context = adal.AuthenticationContext(authority)
  token = context.acquire_token_with_client_credentials(resource_app_id_url, 
                                                        sp_id,
                                                        sp_secret)
  access_token = token['accessToken']

  # Execute query and retrieve result as Spark dataframe
  resultDF = spark.read \
                  .format('com.microsoft.sqlserver.jdbc.spark') \
                  .option('url', azure_sql_url) \
                  .option('query', db_query) \
                  .option('databaseName', db_name) \
                  .option('accessToken', access_token) \
                  .option('encrypt', encrypt) \
                  .option('hostNameInCertification', host_name_in_certificate) \
                  .load()
  
  return(resultDF)
