-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Market Risk Natural Gas: Price Statistics</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

SELECT *
FROM prices_exposed.compared_prices
WHERE price_match = 'False'
LIMIT 10;

-- COMMAND ----------

SELECT pub_date publication_date, publication_name, COUNT(*) number_of_prices
FROM prices_exposed.compared_prices
GROUP BY pub_date, publication_name
ORDER BY publication_date, publication_name;

-- COMMAND ----------

SELECT pub_date publication_date, price_match, COUNT(*) number_of_prices
FROM prices_exposed.compared_prices
GROUP BY pub_date, price_match;
