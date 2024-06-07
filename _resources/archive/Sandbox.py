# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM hive_metastore.dbdemos_lakehouse_iot.turbine
# MAGIC WHERE lat != 'ERROR';

# COMMAND ----------

meta = _sqldf
display(meta)

# COMMAND ----------

meta_small = meta.limit(51*4)
display(meta_small)

# COMMAND ----------

from pyspark.sql.functions import lit

meta_small = meta_small.withColumn('model', lit('LM2500'))
display(meta_small)

# COMMAND ----------

file_location = "/FileStore/tables/ahahn/US_ships_homeport.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
ships = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(ships)

# COMMAND ----------

meta_small = meta_small.drop('location').drop('lat').drop('long').drop('state')

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, floor, lit

# Step 1: Prepare smaller DataFrame (df1)
df1 = ships.withColumn("join_key", monotonically_increasing_id() % 51)

# Step 2: Prepare larger DataFrame (df2)
df2 = meta_small.withColumn("row_num", monotonically_increasing_id())
df2 = df2.withColumn("join_key", floor(df2["row_num"] / 4) % 51).drop("row_num")

# # Step 3: Join the DataFrames
joined_df = df1.join(df2, "join_key").drop("join_key")

# # Result
display(joined_df)

# COMMAND ----------

temp_table_name = "US_ships_homeport"

joined_df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM US_ships_homeport

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE TABLE hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.ship_turbine_meta FROM US_ships_homeport
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.sensor_maintenance (
# MAGIC   fault VARCHAR(255),
# MAGIC   maintenance VARCHAR(255),
# MAGIC   operable BOOLEAN,
# MAGIC   ttr FLOAT,
# MAGIC   parts ARRAY<VARCHAR(255)>
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.sensor_maintenance
# MAGIC (fault, maintenance, operable, ttr, parts)
# MAGIC VALUES
# MAGIC ('sensor_B', 'Ships Force', TRUE, 10.5, ARRAY()),
# MAGIC ('sensor_D', 'Ships Force', TRUE, 5, ARRAY()),
# MAGIC ('sensor_F', 'Depot/I-Level', FALSE, 24, ARRAY());

# COMMAND ----------

# MAGIC %md
# MAGIC THIS TABLE NEEDS TO BE CREATE EITHER DURING THE ML WORKFLOW OR IN THE DLT, CURRENTLY JUST CREATING HERE FOR VISUALIZATION PURPOSES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.ship_current_with_maint_gold AS
# MAGIC SELECT ship.*, maint.*
# MAGIC FROM hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.ship_current_state_gold AS ship
# MAGIC LEFT JOIN hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.sensor_maintenance AS maint ON ship.prediction = maint.fault
# MAGIC WHERE hourly_timestamp = (SELECT max(hourly_timestamp) FROM hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.ship_current_state_gold)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.ship_current_with_maint_gold AS
# MAGIC SELECT ship.*, maint.*
# MAGIC FROM hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.ship_current_state_gold AS ship
# MAGIC LEFT JOIN hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.sensor_maintenance AS maint ON ship.prediction = maint.fault
# MAGIC WHERE hourly_timestamp = (SELECT max(hourly_timestamp) FROM hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.ship_current_state_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and create Parts table from dbfs:/FileStore/tables/ahahn/predictive_maintenance/parts

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hive_metastore.dbdemos_lakehouse_iot_turbine_andrew_hahn.parts AS
# MAGIC SELECT * FROM json.`/FileStore/tables/ahahn/predictive_maintenance/parts`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shipping time table

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

parts = spark.read.table('hive_metastore.dbdemos_navy_turbine_andrew_hahn.part').select('type', 'stock_location').orderBy('type')

display(parts)

# COMMAND ----------

import pandas as pd

ships = spark.read.table('hive_metastore.dbdemos_navy_turbine_andrew_hahn.ship_current_with_maint_gold').select('ship','homeport')

cost = pd.DataFrame(ranked, columns=[s for s in homeport.keys()])
cost['stock_location'] = list(coordinates.keys())

cost = spark.createDataFrame(cost)

cost = cost.selectExpr('stock_location', 'stack(4, "NS Mayport", `NS Mayport`, "NB Norfolk", `NB Norfolk`, "NS Pearl Harbor", `NS Pearl Harbor`, "NB San Diego", `NB San Diego`) as (homeport, cost)')


parts = spark.read.table('hive_metastore.dbdemos_navy_turbine_andrew_hahn.part').select('type', 'stock_location')

cost_ship = cost.join(ships, on='homeport').join(parts, on='stock_location')
cost_ship = cost_ship.groupBy('type', 'stock_location').pivot('ship').agg({'cost': 'first'})

for name in cost_ship.schema.names:
  cost_ship = cost_ship.withColumnRenamed(name, name.replace("USS", "Cost_USS"))

display(cost_ship.orderBy('type'))
# test = cost.join(parts, on='stock_location').orderBy('type')
# display(test)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create supply table

# COMMAND ----------

supply = spark.read.table('hive_metastore.dbdemos_navy_turbine_andrew_hahn.part').select('stock_location', 'stock_available', 'type')
supply = supply.groupBy('type').pivot('stock_location').agg({'stock_available':'first'})
for name in supply.schema.names:
  supply = supply.withColumnRenamed(name, name.replace("FLC", "Supply_FLC"))
display(supply)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create demand table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parts_demand AS
# MAGIC SELECT DISTINCT s.ship, s.turbine_id, p.type AS parts 
# MAGIC FROM ship_current_with_maint_gold AS s
# MAGIC LEFT JOIN part p
# MAGIC ON s.prediction IS NOT NULL 
# MAGIC   AND p.sensors IS NOT NULL 
# MAGIC   AND find_in_set(s.prediction, array_join(p.sensors, ',')) > 0;

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
# MAGIC # Join Supply Demand and Cost tables

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
# MAGIC # RUN me to test if new rank matrix fixes the lp problem

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

table_exists = spark.catalog.tableExists("supply_chain_navy.lp_table")
if table_exists:
    spark.sql("DROP TABLE supply_chain_navy.lp_table")

# Step 3: Save the table again using the updated column names
new_lp_table = lp_table.toDF(*new_column_names)
new_lp_table.write.saveAsTable("hive_metastore.supply_chain_navy.lp_table")

