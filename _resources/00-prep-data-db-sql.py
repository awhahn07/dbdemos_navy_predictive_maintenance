# Databricks notebook source
# MAGIC %md 
# MAGIC # Save the data for DBSQL dashboards

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------


spark.sql(
    f""" 
    USE CATALOG {catalog};
    """
    )
spark.sql(
    f""" 
    USE SCHEMA {db};
    """
    )

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.{db}.turbine_training_dataset_ml AS SELECT * FROM {catalog}.{db}.turbine_training_dataset;
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.{db}.sensor_bronze_ml AS SELECT * FROM {catalog}.{db}.sensor_bronze;
    """
)

# COMMAND ----------

# %sql 

# CREATE OR REPLACE TABLE sensor_maintenance (
#   fault VARCHAR(255),
#   maintenance VARCHAR(255),
#   operable BOOLEAN,
#   ttr FLOAT,
#   parts ARRAY<VARCHAR(255)>
# );

# INSERT INTO sensor_maintenance
# (fault, maintenance, operable, ttr, parts)
# VALUES
# ('sensor_B', 'Ships Force', TRUE, 10.5, ARRAY()),
# ('sensor_D', 'Ships Force', TRUE, 5, ARRAY()),
# ('sensor_F', 'Depot/I-Level', FALSE, 24, ARRAY());

# COMMAND ----------

# %sql 

# CREATE OR REPLACE TABLE ship_current_status_gold AS
# SELECT t.turbine_id, t.hourly_timestamp, t.prediction, s.* except(s.turbine_id), m.* FROM turbine_current_status t
# JOIN ship_meta s USING (turbine_id)
# LEFT JOIN sensor_maintenance m ON prediction = m.fault
# WHERE hourly_timestamp = (SELECT max(hourly_timestamp) FROM turbine_current_status)
