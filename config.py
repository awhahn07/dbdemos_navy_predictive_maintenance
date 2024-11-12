# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2Fconfig&demo_name=lakehouse-iot-platform&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-iot-platform%2Fconfig&version=1">

# COMMAND ----------

catalog = "ahahn_demo"
schema = dbName = db = "dbdemos_navy_pdm_datset_test"
volume_name = "navy_raw_landing"


# catalog = dbutils.widgets.get("catalog")
# schema = dbName = db = dbutils.widgets.get("db")
# volume_name = dbutils.widgets.get("volume")
