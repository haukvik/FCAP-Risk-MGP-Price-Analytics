# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest ICIS Assessments and Indices external data using API
# MAGIC 
# MAGIC We connect to ICIS using their REST API, and save the response to bronze storage layer.

# COMMAND ----------

# Defining input parameters
dbutils.widgets.text("p_ingest_date", "")
v_ingest_date = dbutils.widgets.get("p_ingest_date")

dbutils.widgets.text("p_datasource_name", "")
v_datasource_name = dbutils.widgets.get("p_datasource_name")

dbutils.widgets.text("p_end_date", "")
v_end_date = dbutils.widgets.get("p_end_date")

dbutils.widgets.text("p_start_date", "")
v_start_date = dbutils.widgets.get("p_start_date")

# COMMAND ----------

# MAGIC %run "../0 - Includes/configuration"

# COMMAND ----------

# MAGIC %run "../0 - Includes/common_functions"

# COMMAND ----------

# Create folder for ingestion
ingest_path = f'{bronze_folder_path}/{v_ingest_date}/{v_datasource_name}'
dbutils.fs.mkdirs(f'{ingest_path}/000')

# COMMAND ----------

import requests #library to send API requests
import re # required for data cleaning operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve ICIS prices
# MAGIC 
# MAGIC Below we specify the xml requests we send to the ICIS API. Since the data structures are fundamentally different depending on type of price, we need two:
# MAGIC - One for Assessments prices
# MAGIC - One for Index prices

# COMMAND ----------

# REQUEST FOR ASSESSMENTS

# Request for fetching hub prices
# Scope: We pick up prices, which are of the <type> "series-item" (a date is an item of the price curve series)
# Constraints: 
# The "series order" is the publication date, the op="ge" is the starting date from which we will fetch these
# The "period-label" defines the granularity product (e-g Day-Ahead, Month, Quarter, Season, Year)

# Publication dates:
# From date is denoted by op="ge", to date is denoted by op="le" in c:series_order, e.g.:
# <compare field="c:series-order" op="ge" value="2020-05-20"/>

# removed
# <string-contains field="period-label" pattern="*Day*"/>
#  <string-contains field="c:series.methodology.name" pattern="*Assessment*" />

assessments_xml = """
<request xmlns="http://iddn.icis.com/ns/search">
  <scope>
    <type>series-item</type>
  </scope>
  <constraints>
    <compare field="c:series-order" op="ge" value="{0}"/>
    <compare field="c:series-order" op="le" value="{1}"/>
    <string-contains field="c:series.quote-approach.name" pattern="*Assessment*"/>
    <string-contains field="c:series.methodology.name" pattern="*Assessment*" />
    <string-contains field="c:series.publication.name" pattern="European Spot Gas Markets"/>
    <string-contains field="c:series.commodity.name" pattern="Natural Gas"/>
  </constraints>
  <options>
    <max-results>100000</max-results>
    <precision>embed</precision>
  </options>
  <view>
    <field>c:series.commodity.name</field>
    <field>c:series-order</field>
    <field>assessment-low</field>
    <field>assessment-high</field>
    <field>mid</field>
    <field>start-date</field>
    <field>end-date</field>
    <field>period-label</field>
    <field>c:series.timeframe.name</field>
    <field>c:series.currency.name</field>
    <field>c:series.size-unit.name</field>
    <field>c:series.location.name</field>
    <field>c:series.publication.name</field>
    <field>c:series.methodology.name</field>
    <field>c:series.quote-approach.name</field>
    <field>c:series.transaction-type.name</field>
  </view>
</request>"""

# COMMAND ----------

# REQUEST FOR INDICES

# Request for fetching hub prices
# Scope: We pick up prices, which are of the <type> "series-item" (a date is an item of the price curve series)
# Constraints: 
# The "series order" is the publication date, the op="ge" is the starting date from which we will fetch these
# The "period-label" defines the granularity product (e-g Day-Ahead, Month, Quarter, Season, Year)

# Publication dates:
# From date is denoted by op="ge", to date is denoted by op="le" in c:series_order, e.g.:
# <compare field="c:series-order" op="ge" value="2020-05-20"/>

# removed
# <string-contains field="period-label" pattern="*Day*"/>
#  <string-contains field="c:series.methodology.name" pattern="*Assessment*" />

indices_xml = """
<request xmlns="http://iddn.icis.com/ns/search">
  <scope>
    <type>series-item</type>
  </scope>
  <constraints>
    <compare field="c:series-order" op="ge" value="{0}"/>
    <compare field="c:series-order" op="le" value="{1}"/>
    <string-contains field="c:series.quote-approach.name" pattern="*Ind*"/>
    <string-contains field="c:series.publication.name" pattern="European Spot Gas Markets"/>
    <string-contains field="c:series.commodity.name" pattern="Natural Gas"/>
  </constraints>
  <options>
    <max-results>100000</max-results>
    <precision>embed</precision>
  </options>
  <view>
    <field>c:series.commodity.name</field>
    <field>c:series-order</field>
    <field>assessment-low</field>
    <field>assessment-high</field>
    <field>mid</field>
    <field>start-date</field>
    <field>end-date</field>
    <field>period-label</field>
    <field>c:series.timeframe.name</field>
    <field>c:series.currency.name</field>
    <field>c:series.size-unit.name</field>
    <field>c:series.location.name</field>
    <field>c:series.publication.name</field>
    <field>c:series.quote-approach.name</field>
    <field>c:series.transaction-type.name</field>
  </view>
</request>"""

# COMMAND ----------

def parse_request(req_xml):
    """Takes a raw XML request with line breaks, indentations, etc and transforms it to a cleanly formatted string of the request."""
    req_xml = req_xml.splitlines() #split the string into lines
    req_str ='' # initialising new request
    for line in req_xml:
        req_str += line.strip() # for each row remove whitespaces
    return req_str

# COMMAND ----------

# Connect and retrieve ICIS data based on key vault credentials
req_link = 'https://api.icis.com/v1/search'

ass_payload = parse_request(assessments_xml).format(v_start_date, v_end_date)
ind_payload = parse_request(indices_xml).format(v_start_date, v_end_date)

ass_response = requests.post(req_link, data=ass_payload, auth=(dbutils.secrets.get(scope = secret_scope_name, key = 'icis-api-userid'),
                                                       dbutils.secrets.get(scope = secret_scope_name, key = 'icis-api-password')))
ind_response = requests.post(req_link, data=ind_payload, auth=(dbutils.secrets.get(scope = secret_scope_name, key = 'icis-api-userid'),
                                                       dbutils.secrets.get(scope = secret_scope_name, key = 'icis-api-password')))


print('ICIS answers our Assessments request by saying:', ass_response.reason)
print('ICIS answers our Indices request by saying:', ind_response.reason)
print('Remaining daily ICIS API request quota:', int(ind_response.headers['Daily-Request-Quota']) - int(ind_response.headers['Daily-Request-Quota-Consumed']))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Save XML files to Bronze layer

# COMMAND ----------

if ass_response.reason == 'OK':
  ass_file = open(f'/dbfs{ingest_path}/icis_esgm_assessments.xml', 'wb')
  ass_file.write(ass_response.content)
  ass_file.close()

if ind_response.reason == 'OK':
  ind_file = open(f'/dbfs{ingest_path}/icis_esgm_indices.xml', 'wb')
  ind_file.write(ind_response.content)
  ind_file.close()

# COMMAND ----------

# command to end notebook execution
dbutils.notebook.exit("Success")
