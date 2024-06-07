# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
#Empty value will try default: dbdemos with a fallback to hive_metastore
#Specifying a value will not have fallback and fail if the catalog can't be used/created
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("db", "", "Database")

# COMMAND ----------

# MAGIC %md #Initialization notebook 
# MAGIC
# MAGIC This notebook will create the tables for you if they don't exist so that you can run your ML model directly. It's similar to running the DLT pipeline or the Spark version.
# MAGIC You should not run this notebook directly.

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data $catalog=$catalog $db=$db

# COMMAND ----------

def ingest_folder(folder, data_format, table):
  bronze_products = (spark.readStream
                              .format("cloudFiles")
                              .option("cloudFiles.format", data_format)
                              .option("cloudFiles.inferColumnTypes", "true")
                              .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                              .load(folder))

  return (bronze_products.writeStream
                    .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
                    .option("mergeSchema", "true") #merge any new column dynamically
                    .trigger(once = True) #Remove for real time streaming
                    .table(table)) #Table will be created if we haven't specified the schema first
# TODO update tables
# - "current_turbine_metrics" to "turbine_current_status"
# - Add ship_maintenance table
# 
if not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`historical_turbine_status`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`turbine`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`part`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`sensor_bronze`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`current_turbine_metrics`") or \
   not spark._jsparkSession.catalog().tableExists(f"`{catalog}`.`{database}`.`turbine_training_dataset`"):  
  #One of the table is missing, let's rebuild them all
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`historical_turbine_status`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`turbine`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`part`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`sensor_bronze`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`turbine_training_dataset`")
  spark.sql(f"drop table if exists `{catalog}`.`{database}`.`current_turbine_metrics`")

  #drop the checkpoints 
  if cloud_storage_path.count('/') > 3:
    dbutils.fs.rm(cloud_storage_path, True)

  data_folder = '/FileStore/tables/ahahn/predictive_maintenance'
  
  q1 = ingest_folder(f'{data_folder}/historical_turbine_status', 'json', 'historical_turbine_status')
  q2 = ingest_folder(f'{data_folder}/turbine', 'json', 'turbine')
  q3 = ingest_folder(f'{data_folder}/parts', 'json', 'part')
  ingest_folder(f'{data_folder}/incoming_data', 'json', 'sensor_bronze').awaitTermination()

  q1.awaitTermination()
  q2.awaitTermination()
  q3.awaitTermination()

  #Compute std and percentil of our timeserie per hour
  sensors = [c for c in spark.read.table("sensor_bronze").columns if "sensor" in c]
  aggregations = [F.avg("energy").alias("avg_energy")]
  for sensor in sensors:
    aggregations.append(F.stddev_pop(sensor).alias("std_"+sensor))
    aggregations.append(F.percentile_approx(sensor, [0.1, 0.3, 0.6, 0.8, 0.95]).alias("percentiles_"+sensor))

  (spark.table("sensor_bronze")
        .withColumn("hourly_timestamp", F.date_trunc("hour", F.from_unixtime("timestamp")))
        .groupBy('hourly_timestamp', 'turbine_id').agg(*aggregations)
        .write.mode('overwrite').saveAsTable("sensor_hourly"))

  turbine = spark.table("turbine").where("lat != 'ERROR'")
  health = spark.table("historical_turbine_status")
  (spark.table("sensor_hourly")
    .join(turbine, ['turbine_id']).drop("row", "_rescued_data")
    .join(health, ['turbine_id'])
    .drop("_rescued_data")
    .write.mode('overwrite').saveAsTable("turbine_training_dataset"))

  w = Window.partitionBy("turbine_id").orderBy(col("hourly_timestamp").desc())
  #Fake the predictions to generate the tables
  health = spark.table("historical_turbine_status").select("turbine_id", "abnormal_sensor").withColumnRenamed("abnormal_sensor", "prediction")
  (spark.table("sensor_hourly")
    .withColumn("row", F.row_number().over(w))
    .filter(col("row") == 1)
    .join(spark.table('turbine'), ['turbine_id']).drop("row", "_rescued_data")
    .join(health, ['turbine_id'])
    .write.mode('overwrite').saveAsTable("current_turbine_metrics"))
