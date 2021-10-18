# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM prices_exposed.compared_prices
# MAGIC VERSION AS OF 5
# MAGIC WHERE price_match = 'False'

# COMMAND ----------


