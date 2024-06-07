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

# MAGIC %sql 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ship_current_status_gold AS
# MAGIC SELECT t.turbine_id, t.hourly_timestamp, t.prediction, s.* except(s.turbine_id), m.* FROM turbine_current_status t
# MAGIC JOIN ship_meta s USING (turbine_id)
# MAGIC LEFT JOIN sensor_maintenance m ON prediction = m.fault
# MAGIC WHERE hourly_timestamp = (SELECT max(hourly_timestamp) FROM turbine_current_status)
