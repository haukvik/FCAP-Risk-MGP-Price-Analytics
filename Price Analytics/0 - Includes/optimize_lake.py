# Databricks notebook source
# MAGIC %md
# MAGIC ### Optimize Silver tables

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE prices_processed.endur_prices
# MAGIC   ZORDER BY (pub_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE prices_processed.eex_prices
# MAGIC   ZORDER BY (pub_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE prices_processed.icis_esgm_prices
# MAGIC   ZORDER BY (pub_date);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Gold tables

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE prices_exposed.compared_prices
# MAGIC   ZORDER BY (pub_date);

# COMMAND ----------

dbutils.notebook.exit("Success")
