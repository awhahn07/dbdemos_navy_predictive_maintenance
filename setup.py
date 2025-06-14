# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import time
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, dashboards


# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # CREATE RESOURCE FOR NAVY TURBINE DEMO
# MAGIC
# MAGIC ## CREATE ML CLUSTER FOR AUTOML EXPERIMENTS
# MAGIC
# MAGIC Using Shared Autoscaling cluster with ML Runtime 14.3, cluster ID: 0329-145545-rugby794
# MAGIC
# MAGIC ## CREATE DLT PIPELINE
# MAGIC
# MAGIC Create DLT Pipeline for task 01 Data Ingestion
# MAGIC
# MAGIC ## CREATE JOB WORKFLOW
# MAGIC
# MAGIC Create Job for Demo workflow
# MAGIC
# MAGIC ## EXECUTE JOB RUN
# MAGIC
# MAGIC Run created job

# COMMAND ----------

# DBTITLE 1,Init
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

# DBTITLE 1,Create DLT Pipeline
dlt_name = 'dbdemos_dlt_navy_turbine_{}_{}'.format(name['first'], name['last'])
dlt_notebook_paths = [
    f'{notebook_base}/01-Data-Ingestion/01.1-DLT-Navy-Turbine-SQL',
    f'{notebook_base}/01-Data-Ingestion/01.2-DLT-Navy-GasTurbine-SQL-UDF'
]

library_list = [
    pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=path)) for path in dlt_notebook_paths
]

clusters = [
        pipelines.PipelineCluster(
            label="default",
            autoscale=pipelines.PipelineClusterAutoscale.from_dict({
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
                })
        )]

def get_pipeline_id_by_name(workspace_client: WorkspaceClient, pipeline_name: str):
    # List all pipelines
    response = workspace_client.pipelines.list_pipelines()
    
    # Search for the pipeline with the given name
    for pipeline in response:
        if pipeline.name == pipeline_name:
            return pipeline.pipeline_id
    
    # If no pipeline with the given name is found
    raise Exception(f"No pipeline with the name '{pipeline_name}' found.")

try:
  dlt_pipeline = w.pipelines.create(
      name=dlt_name,
      libraries=library_list,
      clusters=clusters,
      target=db,
      photon=False,
      development=True,
      catalog=catalog,
      configuration={
        "catalog": catalog,
        "db": db
      },
  )
  dlt_id = dlt_pipeline.pipeline_id
  print(f'Created Pipeline ID: {dlt_id}, Name: {dlt_name}')
except Exception as e:
  try:
    print('Pipeline not created, checking if exists')
    dlt_id = get_pipeline_id_by_name(w, dlt_name)
    print(f'Pipeline {dlt_name} exists, ID {dlt_id}, using this pipeline')
  except Exception as e2:
    print(e2)
    print(e)

# COMMAND ----------


#TODO Possibly create new cluster for user

job_name = "dbdemos_navy_turbine_{}_{}".format(name['first'], name['last'])

job_clusters = [
  jobs.JobCluster.from_dict({
    "job_cluster_key": "Shared_job_cluster",
    "new_cluster": {
      "spark_version": "15.1.x-cpu-ml-scala2.12",
      "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode",
        "spark.databricks.dataLineage.enabled": "true"
      },
      "custom_tags": {
        "ResourceClass": "SingleNode",
        "project": "dbdemos",
        "demo": "lakehouse-navy-maintenance"
      },
      "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
      },
      "instance_pool_id": "0727-104344-hauls13-pool-uftxk0r6",
      "data_security_mode": "SINGLE_USER",
      "runtime_engine": "STANDARD",
      "num_workers": 0
    }
  }),
  jobs.JobCluster.from_dict({
    "job_cluster_key": "DLT_fix_cluster",
    "new_cluster": {
      "data_security_mode": "DATA_SECURITY_MODE_DEDICATED",
      "custom_tags": {
        "project": "dbdemos",
        "demo": "lakehouse-navy-maintenance"
      },
      "kind": "CLASSIC_PREVIEW",
      "spark_conf": {
        "spark.databricks.dataLineage.enabled": "true"
      },
      "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
      },
      "runtime_engine": "STANDARD",
      "spark_version": "16.2.x-scala2.12",
      "instance_pool_id": "0727-104344-hauls13-pool-uftxk0r6",
      "use_ml_runtime": "true",
      "is_single_node": "true"
    }
  })
]

tasks = [
  jobs.Task.from_dict(
    {
      "task_key": "init_data",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"{notebook_base}/_resources/01-load-data",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "DLT_fix_cluster",
      "timeout_seconds": 0,
      "email_notifications": {}
    }),
  jobs.Task.from_dict({
      "task_key": "start_dlt_pipeline",
      "depends_on": [
        {
          "task_key": "init_data"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "pipeline_task": {
        "pipeline_id": dlt_id,
        "full_refresh": "true"
      },
      "timeout_seconds": 0,
      "email_notifications": {}
    }),
  jobs.Task.from_dict({
      "task_key": "load_dbsql_and_ml_data",
      "depends_on": [
        {
          "task_key": "start_dlt_pipeline"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"{notebook_base}/_resources/00-prep-data-db-sql",
        "base_parameters": {
          "reset_all_data": "false"
        },
        "source": "WORKSPACE"
      },
      # "existing_cluster_id": uc_table_creator,
      "timeout_seconds": 0,
      "email_notifications": {},
    }),
  jobs.Task.from_dict({
      "task_key": "create_feature_and_automl_run",
      "depends_on": [
        {
          "task_key": "load_dbsql_and_ml_data"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"{notebook_base}/04-Data-Science-ML/04.1-automl-iot-turbine-predictive-maintenance",
        "source": "WORKSPACE"
      },
      #Possibly add Share Americas Autoscaling Cluster
      "job_cluster_key": "Shared_job_cluster",
      "timeout_seconds": 0,
      "email_notifications": {}
    }),
  jobs.Task.from_dict({
      "task_key": "register_ml_model",
      "depends_on": [
        {
          "task_key": "create_feature_and_automl_run"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"{notebook_base}/04-Data-Science-ML/04.2-AutoML-best-register-model",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "DLT_fix_cluster",
      "timeout_seconds": 0,
      "email_notifications": {}
    }),
  jobs.Task.from_dict({
      "task_key": "optimize_supply_routing",
      "depends_on": [
        {
          "task_key": "register_ml_model"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": f"{notebook_base}/05-Supply-Optimization/05.1_Optimize_Transportation",
        "source": "WORKSPACE"
      },
      # "existing_cluster_id": uc_table_creator,
      "timeout_seconds": 0,
      "email_notifications": {}
    }),  
]

created_job = w.jobs.create(name=job_name,
                            tasks=tasks,
                            job_clusters=job_clusters)


# # cleanup
# w.jobs.delete(job_id=created_job.job_id)

# COMMAND ----------

run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

# COMMAND ----------

#TODO add dashboard clone 

# dahsboard_uuid = "118f6ea2-f4b9-4ed1-bed1-027efed2ea2a?o=1444828305810485"

# d = w.dashboards.get(dahsboard_uuid)
# serialized = d.as_dict()
