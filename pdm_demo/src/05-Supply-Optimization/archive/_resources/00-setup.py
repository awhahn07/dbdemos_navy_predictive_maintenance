# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./00-global-setup $reset_all_data=$reset_all_data $db=supply_chain_navy $catalog=hive_metastore

# COMMAND ----------

import os
import re 
import mlflow


# COMMAND ----------

print(cloud_storage_path)
print(dbName)

# COMMAND ----------

reset_all = dbutils.widgets.get('reset_all_data')
reset_all_bool = (reset_all == 'true')

# COMMAND ----------

path = cloud_storage_path

dirname = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
filename = "01-data-generator"
if (os.path.basename(dirname) != '_resources'):
  dirname = os.path.join(dirname,'_resources')
generate_data_notebook_path = os.path.join(dirname,filename)

def generate_data():
  dbutils.notebook.run(generate_data_notebook_path, 600, {"reset_all_data": reset_all, "dbName": dbName, "cloud_storage_path": cloud_storage_path})


if reset_all_bool:
  generate_data()
else:
  try:
    dbutils.fs.ls(path)
  except: 
    generate_data()

# COMMAND ----------

mlflow.set_experiment('/Users/{}/supply_chain_optimization'.format(current_user))
