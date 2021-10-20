# Databricks notebook source
# MAGIC %md
# MAGIC # Create and Save Init script to be used at job cluster
# MAGIC 
# MAGIC Purpose: To install required libraries/packages as required by the python scripts in the process.

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/databricks/scripts/")

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/init_script_prices_job_cluster.sh","""
#!/bin/bash
wget --quiet -O /mnt/driver-daemon/jars/spark-mssql-connector_2.12-1.2.0.jar https://search.maven.org/remotecontent?filepath=com/microsoft/azure/spark-mssql-connector_2.12/1.2.0/spark-mssql-connector_2.12-1.2.0.jar
/databricks/python/bin/pip install PySocks
/databricks/python/bin/pip install paramiko
/databricks/python/bin/pip install pyodbc
/databricks/python/bin/pip install sqlalchemy
/databricks/python/bin/pip install beautifulsoup4
/databricks/python/bin/pip install lxml
/databricks/python/bin/pip install fsspec
/databricks/python/bin/pip install adal
""", True)

# COMMAND ----------

# Check that the script exists
display(dbutils.fs.ls("dbfs:/databricks/scripts/init_script_prices_job_cluster.sh"))

# COMMAND ----------


