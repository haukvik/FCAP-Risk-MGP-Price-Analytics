# Databricks notebook source
# Project storage params
storage_account_name = "s184prices"
client_id            = dbutils.secrets.get(scope="prices-scope", key="fcap-sp-id")
tenant_id            = dbutils.secrets.get(scope="prices-scope", key="fcap-sp-tenant-id")
client_secret        = dbutils.secrets.get(scope="prices-scope", key="fcap-sp-secret")

# COMMAND ----------

# Project storage config
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# Function to mount abss storage
def mount_adls(storage_account_name, container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)
  print(f'/mnt/{storage_account_name}/{container_name} has been mounted.')

# COMMAND ----------

mount_adls(storage_account_name, 'bronze')

# COMMAND ----------

mount_adls(storage_account_name, 'silver')

# COMMAND ----------

mount_adls(storage_account_name, 'gold')

# COMMAND ----------

# check the storages are mounted
print(type(dbutils.fs.ls(f"/mnt/{storage_account_name}/bronze"))==list)
print(type(dbutils.fs.ls(f"/mnt/{storage_account_name}/silver"))==list)
print(type(dbutils.fs.ls(f"/mnt/{storage_account_name}/gold"))==list)
