# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook editor class
# MAGIC - Takes catalog name and db/schema name arguments
# MAGIC - Call convert() method to edit hardcoded paths in 01.1 and 01.2 notebooks to point to local users Volume and model registry
# MAGIC

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import base64
from pathlib import Path

class NotebookEditor:
  def __init__(self, catalog, db):
    self.w = WorkspaceClient()
    self.username = self.w.current_user.me().user_name
    self.catalog = catalog
    self.db = db

    current_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_base = str(Path(current_notebook).parent)

    self.notebook_locations = [
      f'{notebook_base}/01-Data-Ingestion/01.1-DLT-Navy-Turbine-SQL',
      f'{notebook_base}/01-Data-Ingestion/01.2-DLT-Navy-GasTurbine-SQL-UDF'
    ]

# Read notebook source, output string
  def notebook_reader(self, file_location):
    export_response = self.w.workspace.export(file_location, format=workspace.ExportFormat.SOURCE)
    return base64.b64decode(export_response.content).decode()

  # Write notebook source to a file
  def write_notebook(self, notebook_path, nb_str, language=workspace.Language.SQL):
    try:
      self.w.workspace.import_(content=nb_str,
                          format=workspace.ImportFormat.SOURCE,
                          language=language,
                          overwrite=True,
                          path=notebook_path)
      print(f"Wrote {notebook_path}")
    except Exception as e:
      print(f"Failed write: {e}")

  # Convert notebook source to base64 string
  @staticmethod
  def prep_nb_str(nb_str):
    return base64.b64encode(nb_str.encode()).decode()

  # Convert volume paths to current user
  def convert_volume_paths(self, nb_str):
    return nb_str.replace("/Volumes/ahahn_demo/dbdemos_navy_pdm/", f"/Volumes/{self.catalog}/{self.db}/")
  
  # Convert model paths to current user
  def convert_model_path(self, nb_str):
    return nb_str.replace("models:/ahahn_demo.dbdemos_navy_pdm.", f"models:/{self.catalog}.{self.db}.")

  def convert_notebook(self, file_location):
    if "01.1" in file_location:
      language = workspace.Language.SQL
    elif "01.2" in file_location:
      language = workspace.Language.PYTHON
    
    nb_str = self.notebook_reader(file_location)
    nb_str = self.convert_volume_paths(nb_str)
    nb_str = self.convert_model_path(nb_str)
    nb_str = self.prep_nb_str(nb_str)
    self.write_notebook(file_location, nb_str, language)

  def convert(self):
    for location in self.notebook_locations:
      self.convert_notebook(location)
    print(f"Converted Volume paths to:\t /Volumes/{self.catalog}/{self.db}\nConverted Model paths to:\t models:/{self.catalog}.{self.db}")
