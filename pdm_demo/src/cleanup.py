# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop demo schema

# COMMAND ----------

dbutils.widgets.dropdown("reset_option", "reset_all_data", ["reset_all_data", "drop_tables_only", "drop_tables_and_pipeline"], "Reset Option")
reset_option = dbutils.widgets.get("reset_option")

# COMMAND ----------

if reset_option == "reset_all_data":
  drop_schema = f"DROP SCHEMA IF EXISTS {catalog}.{db} CASCADE"
  spark.sql(drop_schema)
  
if reset_option == "drop_tables_only":  
  tables = spark.sql(f"SHOW TABLES IN {catalog}.{db}").select("tableName").collect()
  for table in tables:
      drop_table = f"DROP TABLE IF EXISTS {catalog}.{db}.{table['tableName']}"
      spark.sql(drop_table)


# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, dashboards

import re
from pathlib import Path

#Instantiate workspace client
w = WorkspaceClient()

# Get current user name 
user_name = w.current_user.me().user_name

# Create base notebook path (path to project directory in workspace) 
current_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_base = str(Path(current_notebook).parent)

# Get user First and Last Name
name_regex = r'^(\w+)\.(\w+)@'
match = re.match(name_regex, user_name)
if match:
  name = {'first': match.group(1), 'last': match.group(2)}
else:
  raise "Unable to extract user name"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete DLT pipeline

# COMMAND ----------

# Get DLT Pipeline
def get_pipeline_id_by_name(workspace_client: WorkspaceClient, pipeline_name: str):
    # List all pipelines
    response = workspace_client.pipelines.list_pipelines()
    
    # Search for the pipeline with the given name
    for pipeline in response:
        if pipeline.name == pipeline_name:
            return pipeline.pipeline_id
    
    # If no pipeline with the given name is found
    raise Exception(f"No pipeline with the name '{pipeline_name}' found.")

dlt_name = 'dbdemos_dlt_navy_turbine_{}_{}'.format(name['first'], name['last'])
dlt_id = get_pipeline_id_by_name(w, dlt_name)

try:
  w.pipelines.delete(pipeline_id=dlt_id)
  print(f'Pipeline {dlt_id} deleted')
except:
  print(f'Pipeline deletion failed for pipline name {dlt_id}, delete manually')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete job workflow (todo)

# COMMAND ----------


