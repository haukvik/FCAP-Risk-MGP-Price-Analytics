# Databricks notebook source
# MAGIC %md
# MAGIC # Fetching Data from External source: EEX Datasource
# MAGIC 
# MAGIC This script connects to the SFTP server of EEX and fetches relevant csv price files.

# COMMAND ----------

# Defining input parameters
dbutils.widgets.text("p_ingest_date", "2021-10-14")
v_ingest_date = dbutils.widgets.get("p_ingest_date")

dbutils.widgets.text("p_datasource_name", "EEX DataSource SFTP")
v_datasource_name = dbutils.widgets.get("p_datasource_name")

dbutils.widgets.text("p_end_date", "2021-10-13")
v_end_date = dbutils.widgets.get("p_end_date")

dbutils.widgets.text("p_start_date", "2021-10-13")
v_start_date = dbutils.widgets.get("p_start_date")

# COMMAND ----------

# MAGIC %run "../0 - Includes/configuration"

# COMMAND ----------

# Create folder for ingestion
ingest_path = f'{bronze_folder_path}/{v_ingest_date}/{v_datasource_name}'
dbutils.fs.mkdirs(f'{ingest_path}/000')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Establish SFTP connection to EEX Datasource <a class="anchor" id="connect"></a>

# COMMAND ----------

import paramiko
import socks #pysocks
import pandas as pd
from datetime import timedelta, date
import datetime

# COMMAND ----------

# Assign dates to given scope
start_date = datetime.datetime.strptime(v_start_date, '%Y-%m-%d').date()
end_date = datetime.datetime.strptime(v_end_date, '%Y-%m-%d').date()

# COMMAND ----------

# Function to create a list of the publication dates to retrieve
def daterange(start_date, end_date):
  date_list = []
  for n in range(int ((end_date - start_date).days)+1):
      date_list.append( start_date + timedelta(n) )
  return date_list

# COMMAND ----------

# Creating the list of publication dates
publication_dates = daterange(start_date, end_date)

# COMMAND ----------

print('\nSTARTING EEX PRICE FETCH\n')

print(' - Connecting to EEX DataSource SFTP...')

# Setting up connection to EEX

sock = socks.socksocket()
host, port = 'datasource.eex-group.com', 22

# Connect the socket
sock.connect((host, port))

# Creating the Paramiko Transport instace to connect to the source
transport = paramiko.Transport(sock)
transport.connect(
    username=dbutils.secrets.get(scope = secret_scope_name, key = 'eex-sftp-userid'),
    password=dbutils.secrets.get(scope = secret_scope_name, key = 'eex-sftp-password'),
)
eex = paramiko.SFTPClient.from_transport(transport)

# Get FTP basepath
base_path = eex.normalize('.')

print(' - Connection established.')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Retrieve the defined price publications from EEX Datasource SFTP <a class="anchor" id="dirinfo"></a>

# COMMAND ----------

# Define price parameters
ingest_hubs = ['the', 'nbp', 'peg', 'ttf', 'zee', 'ztp'] # only relevant hubs for retrival performance
ingest_products = ['spot', 'derivatives'] # define upfront for performance

# COMMAND ----------

def ingestEEXPrices(publication_date, save_path):
  publication_date_str = publication_date.strftime('%Y%m%d')
  missing_files = []
  missing_folders = []
  for hub_name in ingest_hubs:
    for product_name in ingest_products:
      local_path = save_path
      remote_path = f"{base_path}/market_data/natgas/{hub_name}/{product_name}/csv/{str(publication_date.year)}/{publication_date_str}"
      try:
        for file_name in eex.listdir(remote_path):
          if 'MarketResults' in file_name: # we only want MarketResults files
            try:
              eex.get(f"{remote_path}/{file_name}", f"{save_path}/{file_name}")
            except:
              missing_files.append(remote_path)
      except:
        missing_folders.append(remote_path)

# COMMAND ----------

for publication_date in publication_dates:
  ingestEEXPrices(publication_date, f"/dbfs{ingest_path}")

# COMMAND ----------

# Being polite and closing the SFTP connection
eex.close()

# COMMAND ----------

print(f'Successfully downloaded {len(dbutils.fs.ls(ingest_path))} price files.')

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")
