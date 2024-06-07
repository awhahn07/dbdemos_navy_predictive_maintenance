# Databricks notebook source
# MAGIC %md
# MAGIC # Fine Grained Demand Forecasting

# COMMAND ----------

# MAGIC %md
# MAGIC Prerequisite: 
# MAGIC   - Make sure to run Predictive maintenance pipeline before running. This notebook depends on prediction outputs
# MAGIC   - Make sure to run 01_Introduction_And_Setup before running this notebook.
# MAGIC
# MAGIC In this notebook we 
# MAGIC   - Pull the predictions from the predictive maintenance demo to create demand forecasting table.
# MAGIC   - Create shipping cost, supply, and demand tables for the LP problem
# MAGIC   - We then aggregate cost, supply, and demand for each product.
# MAGIC
# MAGIC Key highlights for this notebook:
# MAGIC
# MAGIC Use Databricks' collaborative and interactive notebook environment to find an appropriate time series mdoel
# MAGIC Use Pandas UDFs (user-defined functions) to take your single-node data science code, and distribute it across multiple nodes

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

import os
import datetime as dt
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt

import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shipping time table
# MAGIC
# MAGIC - Use distance between Stock location and homeport to determine cost table and rank in order. 
# MAGIC - Replace zero rank with 0.1 cost for LP problem

# COMMAND ----------

import numpy as np
from math import radians, cos, sin, asin, sqrt

# Haversine formula to calculate distance between two lat/long points
def haversine(lon1, lat1, lon2, lat2):
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return c * r

# Define the latitudes and longitudes for each location
# Example: {'FLC Jacksonville': (latitude, longitude), ...}
coordinates = {
    'FLC Jacksonville': (30.39349, -81.410574),
    'FLC Norfolk': (36.945, -76.313056),
    'FLC Pearl Harbor': (21.3647, -157.9498),
    'FLC Puget Sound': (47.6, -122.4),
    'FLC San Diego': (32.684722, -117.13)
}

homeport = {
    'NS Mayport': (30.4, -81.4),
    'NB Norfolk': (36.9, -76.3),
    'NS Pearl Harbor': (21.4, -157.9),
    'NB San Diego': (32.7, -117.1)
}
# Initialize an empty adjacency matrix
n = len(coordinates)
m = len(homeport)
adjacency_matrix = np.zeros((n, m))

# Populate the matrix with distances
flc_locations = list(coordinates.keys())
hp_locations = list(homeport.keys())

for i in range(n):
    for j in range(m):
        lat1, lon1 = coordinates[flc_locations[i]]
        lat2, lon2 = homeport[hp_locations[j]]
        distance = haversine(lon1, lat1, lon2, lat2)
        if distance == 0:
            distance = 0.1
        adjacency_matrix[i][j] = distance

# Print the adjacency matrix
ranked = np.zeros_like(adjacency_matrix)

for row_ind, row in enumerate(adjacency_matrix):
  tmp = {dis: i for i, dis in enumerate(sorted(row))}
  for col_ind in range(len(row)):
    if tmp[row[col_ind]] == 0:
        ranked[row_ind][col_ind] = 0.1
    else:    
        ranked[row_ind][col_ind] = tmp[row[col_ind]]

print(ranked) 



# COMMAND ----------

parts = spark.read.table(f'{catalog}.{db}.parts').select('type', 'stock_location').orderBy('type')
display(parts)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Create Cost table from shipping time table

# COMMAND ----------

ships = spark.read.table(f'{catalog}.{db}.ship_meta').select('ship','homeport').distinct()

cost = pd.DataFrame(ranked, columns=[s for s in homeport.keys()])
cost['stock_location'] = list(coordinates.keys())

cost = spark.createDataFrame(cost)

cost = cost.selectExpr('stock_location', 'stack(4, "NS Mayport", `NS Mayport`, "NB Norfolk", `NB Norfolk`, "NS Pearl Harbor", `NS Pearl Harbor`, "NB San Diego", `NB San Diego`) as (homeport, cost)')


cost_ship = cost.join(ships, on='homeport').join(parts, on='stock_location')
cost_ship = cost_ship.groupBy('type', 'stock_location').pivot('ship').agg({'cost': 'first'})

for name in cost_ship.schema.names:
  cost_ship = cost_ship.withColumnRenamed(name, name.replace("USS", "Cost_USS"))

display(cost_ship.orderBy('type'))


# COMMAND ----------

# MAGIC %md
# MAGIC # Create supply table

# COMMAND ----------


# supply = spark.read.table('hive_metastore.dbdemos_navy_turbine_andrew_hahn.part').select('stock_location', 'stock_available', 'type')
supply = spark.read.table(f'{catalog}.{db}.parts').select('stock_location', 'stock_available', 'type')
supply = supply.groupBy('type').pivot('stock_location').agg({'stock_available':'first'})
for name in supply.schema.names:
  supply = supply.withColumnRenamed(name, name.replace("FLC", "Supply_FLC"))
display(supply)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create demand table

# COMMAND ----------

spark.sql(
  f"""
  USE CATALOG {catalog}
  """
  )
spark.sql(
  f"""
  USE SCHEMA {db}
  """
  )

# COMMAND ----------


query = f"""
CREATE OR REPLACE TEMP VIEW parts_demand AS
SELECT DISTINCT s.ship, s.turbine_id, p.type AS parts 
FROM {catalog}.{db}.ship_current_status_gold AS s
LEFT JOIN {catalog}.{db}.parts p
ON s.prediction IS NOT NULL 
  AND p.sensors IS NOT NULL 
  AND find_in_set(s.prediction, array_join(p.sensors, ',')) > 0;
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parts_demand

# COMMAND ----------

import pyspark.sql.functions as f

demand = spark.sql("SELECT ship, parts, COUNT(parts) AS demand FROM parts_demand GROUP BY ship, parts ORDER BY ship")

demand = demand.groupBy('parts').pivot("ship").agg(f.first("demand").alias("demand"))

for name in demand.schema.names:
  if name == 'parts':
    demand = demand.withColumnRenamed(name, name.replace("parts", "type"))
  demand = demand.withColumnRenamed(name, name.replace("USS", "Demand_USS"))

demand = demand.filter(demand.type.isNotNull())

display(demand)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Supply Demand and Cost tables
# MAGIC
# MAGIC - Join supply, demand, and cost tables
# MAGIC - Fill null values with 0, i.e. no supply or demand

# COMMAND ----------

import re

lp_table = (cost_ship.
            join(supply, ["type"], how='inner').
            join(demand, ["type"], how='inner').
            fillna(0)
)

pattern = pattern = r"\(.*?\)"
for name in lp_table.schema.names:
  new = re.sub(pattern, "", name).replace(" ", "_")
  lp_table = lp_table.withColumnRenamed(name, new)

display(lp_table.orderBy('type'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up names for Optimization using PuLP
# MAGIC
# MAGIC - PuLP is very senstative to naming conventions when creating the LP problem to optimize. Ensure all Column names and field names from pivot table match
# MAGIC * NOTE: In the case of failure during LP optimization, check the regex string matching to ensure no mismatches  

# COMMAND ----------

# Step 1: Identify the invalid characters
invalid_chars = [' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=', '.', '-', "'"]

# Step 2: Rename the columns with valid characters
new_column_names = [col for col in lp_table.columns]
for invalid_char in invalid_chars:
  if invalid_char == '-':
    new_column_names = [col.replace(invalid_char, '_') for col in new_column_names]
  else:
    new_column_names = [col.replace(invalid_char, '') for col in new_column_names]

new_lp_table = lp_table.toDF(*new_column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write supply-demand-cost-df to Delta

# COMMAND ----------

# Convert the delta table to Delta Live Table
new_lp_table.createOrReplaceTempView("temp_table")
spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{db}.lp_table AS SELECT * FROM temp_table")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{db}.lp_table"))

# COMMAND ----------

# MAGIC %md 
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | pulp                                 | A python Linear Programming API      | https://github.com/coin-or/pulp/blob/master/LICENSE        | https://github.com/coin-or/pulp                      |
