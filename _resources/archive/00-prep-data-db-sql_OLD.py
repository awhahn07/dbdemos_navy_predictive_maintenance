# Databricks notebook source
# MAGIC %md 
# MAGIC # Save the data for DBSQL dashboards

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# MAGIC %run ./00-load-tables $reset_all_data=$reset_all_data $catalog=hive_metastore $db=dbdemos_navy_turbine_andrew_hahn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modify Turbine Table for Navy Dashboard
# MAGIC
# MAGIC Take existing turbine meta data and IDs, filtered where lat != error
# MAGIC
# MAGIC Select Number of ships (51) X Num turbines per ship (4) 
# MAGIC
# MAGIC Assign 4 turbines per ship in output meta data table
# MAGIC
# MAGIC **NOTE** original metadata is kept as model was trained on fields that are dropped for this dashboard. This can be updated in later versions to train new model but kept for now
# MAGIC
# MAGIC Ship metadata is manually assigned to current status metrics based on prediciton to make output visualization look more realistic, i.e. not randomly shuffled so status relfects higher overall ship readiness which is likely expected. This can be changed in later iterarions

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id, floor, lit

# Get ship meta data from csv
file_location = "/FileStore/tables/ahahn/US_ships_homeport.csv"
ships = spark.read.format("csv") \
  .option("inferSchema", "false") \
  .option("header", "true") \
  .option("sep", ",") \
  .load(file_location)
ships = ships.withColumn("join_key", monotonically_increasing_id() % 51)

cols = ['turbine_id','prediction','country']
c = (spark.table('current_turbine_metrics').select(*cols).withColumn('model', lit('LM2500')))
c = c.orderBy('prediction').withColumn("row_num", monotonically_increasing_id())
c = c.withColumn("join_key", floor(c["row_num"] / 4) % 51).drop("row_num")
joined = ships.join(c, 'join_key').drop('join_key').drop('prediction')

# filtered = filtered.withColumn("row_num", monotonically_increasing_id())
# filtered = filtered.withColumn("join_key", floor(filtered["row_num"] / 4) % 51).drop("row_num")
# joined = ships.join(filtered, "join_key").drop("join_key")

# Create new table for ship meta data
joined.write.format("delta").mode("overwrite").saveAsTable("ship_turbine_meta")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE sensor_maintenance (
# MAGIC   fault VARCHAR(255),
# MAGIC   maintenance VARCHAR(255),
# MAGIC   operable BOOLEAN,
# MAGIC   ttr FLOAT,
# MAGIC   parts ARRAY<VARCHAR(255)>
# MAGIC );
# MAGIC
# MAGIC INSERT INTO sensor_maintenance
# MAGIC (fault, maintenance, operable, ttr, parts)
# MAGIC VALUES
# MAGIC ('sensor_B', 'Ships Force', TRUE, 10.5, ARRAY()),
# MAGIC ('sensor_D', 'Ships Force', TRUE, 5, ARRAY()),
# MAGIC ('sensor_F', 'Depot/I-Level', FALSE, 24, ARRAY());

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create table for queires and dashboard

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ship_current_with_maint_gold AS
# MAGIC SELECT t.turbine_id, t.hourly_timestamp, t.prediction, s.* except(s.turbine_id), m.* FROM current_turbine_metrics t
# MAGIC JOIN ship_turbine_meta s USING (turbine_id)
# MAGIC LEFT JOIN sensor_maintenance m ON prediction = m.fault
# MAGIC WHERE hourly_timestamp = (SELECT max(hourly_timestamp) FROM current_turbine_metrics)
