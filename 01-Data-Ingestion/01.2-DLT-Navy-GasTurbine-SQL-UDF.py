# Databricks notebook source
# DBTITLE 1,Let's install mlflow to load our model
# MAGIC %pip install mlflow==2.22.0
# MAGIC # %pip install azure-core azure-storage-file-datalake #for the display() in Azure only
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md #Registering python UDF to a SQL function
# MAGIC This is a companion notebook to load the wind turbine prediction model as a spark udf and save it as a SQL function
# MAGIC  
# MAGIC Make sure you add this notebook in your DLT job to have access to the `get_turbine_status` function. (Currently mixing python in a SQL DLT notebook won't run the python)
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=local&notebook=%2F01-Data-ingestion%2F01.2-DLT-Wind-Turbine-SQL-UDF&demo_name=lakehouse-iot-platform&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-iot-platform%2F01-Data-ingestion%2F01.2-DLT-Wind-Turbine-SQL-UDF&version=1">

# COMMAND ----------

import mlflow
mlflow.set_registry_uri('databricks-uc')
catalog = spark.conf.get("catalog")
db = spark.conf.get("db")
#                                                                                                       Stage/version  
#                                                                                    Model name             |        
#                                                                                         |                 |        
predict_maintenance_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{db}.navy_turbine_maintenance@prod", "string",env_manager='virtualenv')
spark.udf.register("predict_maintenance", predict_maintenance_udf)

# COMMAND ----------

# MAGIC %md ### Setting up the DLT 
# MAGIC
# MAGIC This notebook must be included in your DLT "libraries" parameter:
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC     "id": "95f28631-1884-425e-af69-05c3f397dd90",
# MAGIC     "name": "xxxx",
# MAGIC     "storage": "/demos/dlt/lakehouse_iot_wind_turbine/xxxxx",
# MAGIC     "configuration": {
# MAGIC         "pipelines.useV2DetailsPage": "true"
# MAGIC     },
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/xxxx/01.1-DLT-Wind-Turbine-SQL"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/xxxx/01.2-DLT-Wind-Turbine-SQL-UDF"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "target": "retail_lakehouse_churn_xxxx",
# MAGIC     "continuous": false,
# MAGIC     "development": false
# MAGIC }
# MAGIC ```
