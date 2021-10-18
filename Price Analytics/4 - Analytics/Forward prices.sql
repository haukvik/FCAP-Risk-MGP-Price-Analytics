-- Databricks notebook source
SELECT *
FROM prices_processed.icis_esgm_prices
LIMIT 10

-- COMMAND ----------

SELECT max(pub_date)
from prices_processed.icis_esgm_prices;

-- COMMAND ----------

SELECT avg(price_mid)
FROM prices_processed.icis_esgm_prices
WHERE pub_date = (SELECT max(pub_date) FROM prices_processed.icis_esgm_prices)
  AND product
GROUP BY location_name, period_rel, end_date
HAVING location_name = 'NBP'

-- COMMAND ----------


