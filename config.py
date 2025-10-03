# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2Fconfig&demo_name=lakehouse-iot-platform&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-iot-platform%2Fconfig&version=1">

# COMMAND ----------

dbutils.widgets.dropdown("sector", "pubsec", ["pubsec", "navy"], "Select Sector")

# COMMAND ----------

# MAGIC %md
# MAGIC # Set Catalog, DB, Volume

# COMMAND ----------

catalog = "public_sector"
# schema = dbName = db = "predictive_maintenance_pubsec"
volume_name = "raw_landing"

# COMMAND ----------

# MAGIC %md
# MAGIC # Set Demo Type

# COMMAND ----------

demo_type = "navy"
print(f"demo_type: {demo_type}")

if demo_type == "pubsec":
  schema = dbName = db = "predictive_maintenance_pubsec"
elif demo_type == "DoD":
  schema = dbName = db = "predictive_maintenance_DoD"
elif demo_type == "navy":
  schema = dbName = db = "predictive_maintenance_navy_test"

print(f"schema: {schema}")
print(f"dbName: {dbName}")
print(f"db: {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Set ML Variables

# COMMAND ----------

experiment_name = "pubsec_predictive_maintenance"
print(f"experiment_name: {experiment_name}")

model_name = "predictive_maintenance_model"
print(f"model_name: {model_name}")
