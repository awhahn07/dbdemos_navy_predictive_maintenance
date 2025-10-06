# Databricks notebook source


# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, dashboards

import re
from pathlib import Path


class NavyDemoClient():
  def __init__(self):
    #Workspace Client
    self.w = WorkspaceClient()

    #User Information
    self.user_name = self.w.current_user.me().user_name

    name_dict = self.get_user_data()
    self.user_first = name_dict['first']
    self.user_last = name_dict['last']

    # Notebook Paths
    self.notebook_base = self._get_notebook_path()

    # Pipeline Paths
    self.pipeline_id = self.get_pipeline_id_by_name()

  def _get_user_data(self):
    # Get user First and Last Name
    name_regex = r'^(\w+)\.(\w+)@'
    match = re.match(name_regex, self.user_name)
    if match:
      name = {'first': match.group(1), 'last': match.group(2)}
      return name
    else:
      raise "Unable to extract user name"
  
  @staticmethod
  def _get_notebook_path(self):
    current_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    return str(Path(current_notebook).parent)
  
  def get_pipeline_id_by_name(self, dlt_name:str = None):
    # List all pipelines
    response = self.w.pipelines.list_pipelines()
    
    if dlt_name is None:
      dlt_name = 'dbdemos_dlt_navy_turbine_{}_{}'.format(self.user_first, self.user_last)

    # Search for the pipeline with the given name
    for pipeline in response:
        if pipeline.name == pipeline_name:
            return pipeline.pipeline_id
    
    # If no pipeline with the given name is found
    raise Exception(f"No pipeline with the name '{pipeline_name}' found.")


