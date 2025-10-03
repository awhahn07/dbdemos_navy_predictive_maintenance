# Databricks notebook source
# MAGIC %pip install git+https://github.com/QuentinAmbard/mandrova faker databricks-sdk==0.40.0 mlflow==2.22.0 numpy==1.26.4 pandas==2.2.3
# MAGIC
# MAGIC # Restart the Python environment
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ./00-global-setup-v2

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
folder = f"/Volumes/{catalog}/{db}/{volume_name}"

data_exists = False
try:
  #TODO update for new data versions
  dbutils.fs.ls(folder)
  dbutils.fs.ls(folder+"/historical_turbine_status")
  dbutils.fs.ls(folder+f"/parts_{demo_type}")
  dbutils.fs.ls(folder+"/turbine")
  dbutils.fs.ls(folder+"/incoming_data")
  dbutils.fs.ls(folder+f"/meta_{demo_type}")
  data_exists = True
  print("data already exists")
except Exception as e:
  print(f"folder doesn't exists, generating the data...")

# COMMAND ----------

# Add renove after tag to schema so talbes dont get deleted for 90 days
from datetime import datetime, timedelta

def get_date_plus_90_days():
    current_date = datetime.now()
    future_date = current_date + timedelta(days=90)
    return future_date.strftime('%Y%m%d')
  
tag_key = 'RemoveAfter'
tag_val = get_date_plus_90_days()

sql = f"""ALTER SCHEMA {catalog}.{db} SET TAGS ('{tag_key}'='{tag_val}')"""
spark.sql(sql)

# COMMAND ----------

git_profile = "awhahn07"
git_repo = "dbdemos-fed-datasets"
git_root_folder = "pdm_data"

data_folders = [
  "historical_turbine_status",
  "turbine",
  "incoming_data",
  f"meta_{demo_type}",
  f"parts_{demo_type}"
]

def download_data(volume_folder, git_profile, git_repo, git_root_folder, data_folders):
  for folder in data_folders:
    DBDemos.download_file_from_git(volume_folder+'/'+folder, git_profile, git_repo, '/'+git_root_folder+'/'+folder)

data_downloaded = False

###TODO Remove in Prod 

reset_all_data = True

if not data_exists:
  if not reset_all_data:
    try:
      # TODO Update git data sources
      download_data(folder, git_profile, git_repo, git_root_folder, data_folders)
        # DBDemos.download_file_from_git(folder+'/historical_turbine_status', "awhahn07", "dbdemos-fed-datasets", "/new_pdm_data/historical_turbine_status")
        # DBDemos.download_file_from_git(folder+'/turbine', "awhahn07", "dbdemos-fed-datasets", "/new_pdm_data/turbine")
        # DBDemos.download_file_from_git(folder+'/incoming_data', "awhahn07", "dbdemos-fed-datasets", "/new_pdm_data/incoming_data")
        # DBDemos.download_file_from_git(folder+f'/meta_{demo_type}', "awhahn07", "dbdemos-fed-datasets", f"/new_pdm_data/meta_{demo_type}")
        # DBDemos.download_file_from_git(folder+f'/parts_{demo_type}', "awhahn07", "dbdemos-fed-datasets", f"/new_pdm_data/parts_{demo_type})")    
      data_downloaded = True
    except Exception as e: 
        print(f"Error trying to download the file from the repo: {str(e)}. Will generate the data instead...")    

# COMMAND ----------

#As we need a model in the DLT pipeline and the model depends of the DLT pipeline too, let's build an empty one.
#This wouldn't make sense in a real-world system where we'd have 2 jobs / pipeline (1 for ingestion, and 1 to build the model / run inferences)
import random
import mlflow
from  mlflow.models.signature import ModelSignature
import pandas as pd
import numpy as np
import cloudpickle
from unittest import mock

# define a custom model randomly flagging 10% of sensor for the demo init (it'll be replace with proper model on the training part.)
class MaintenanceEmptyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        import random
        sensors = ['sensor_F', 'sensor_D', 'sensor_B']  # List of sensors
        return model_input['avg_energy'].apply(lambda x: 'ok' if random.random() < 0.9 else random.choice(sensors))
 
#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')

#Only register empty model if model doesn't exist yet
client = mlflow.tracking.MlflowClient()
try:
  latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
except Exception as e:
    print(e)
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print("Model doesn't exist - saving an empty one")
        # setup the experiment folder
        DBDemos.init_experiment_for_batch(model_name, "predictive_maintenance_mock")
        # save the model
        churn_model = MaintenanceEmptyModel()
        import pandas as pd
        
        signature = ModelSignature.from_dict({'inputs': '[{"name": "hourly_timestamp", "type": "datetime"}, {"name": "avg_energy", "type": "double"}, {"name": "std_sensor_A", "type": "double"}, {"name": "std_sensor_B", "type": "double"}, {"name": "std_sensor_C", "type": "double"}, {"name": "std_sensor_D", "type": "double"}, {"name": "std_sensor_E", "type": "double"}, {"name": "std_sensor_F", "type": "double"}, {"name": "percentiles_sensor_A", "type": "string"}, {"name": "percentiles_sensor_B", "type": "string"}, {"name": "percentiles_sensor_C", "type": "string"}, {"name": "percentiles_sensor_D", "type": "string"}, {"name": "percentiles_sensor_E", "type": "string"}, {"name": "percentiles_sensor_F", "type": "string"}]','outputs': '[{"type": "tensor", "tensor-spec": {"dtype": "object", "shape": [-1]}}]'})
        
        with mlflow.start_run(run_name="mockup_model") as run, mock.patch("mlflow.utils.environment.PYTHON_VERSION", DBDemos.get_python_version_mlflow()):
            model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=churn_model, signature=signature, pip_requirements=['mlflow=='+mlflow.__version__, 'pandas=='+pd.__version__, 'numpy=='+np.__version__, 'cloudpickle=='+cloudpickle.__version__])

        #Register & move the model in production
        model_registered = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=model_registered.version)
        latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
    else:
        raise e
        # print(f"ERROR: couldn't access model for unknown reason - DLT pipeline will likely fail as model isn't available: {e}")



# COMMAND ----------

if data_downloaded:
    dbutils.notebook.exit(f"Data Downloaded to {folder}")
elif data_exists==True and reset_all_data==False: 
    dbutils.notebook.exit(f"Data Downloaded to {folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data if necessary

# COMMAND ----------

# DBTITLE 1,Create Sensor Maintenance Table
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, ArrayType

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {db}")

# TODO remove parts field (after eval)
sensor_schema = StructType([
    StructField("fault", StringType(), True),
    StructField("maintenance_type", StringType(), True),
    StructField("operable", BooleanType(), True),
    StructField("ttr", FloatType(), True),
])

data = [
    ('sensor_A', 'Intermediate Level', True, 8.0),    # Compressor related - moderate impact
    ('sensor_B', 'Intermediate Level', True, 10.5),   # Existing - moderate impact  
    ('sensor_C', 'Organizational Level', True, 6.0),    # Cooling system - lower impact
    ('sensor_D', 'Organizational Level', True, 5.0),    # Existing - lower impact
    ('sensor_E', 'Intermediate Level', True, 12.0),   # Fuel system - moderate impact
    ('sensor_F', 'Depot Level', False, 24.0) # Existing - high impact (turbine blades/vanes)
]

df = spark.createDataFrame(data, sensor_schema)
df.write.mode("overwrite").saveAsTable("sensor_maintenance")

# COMMAND ----------

# MAGIC %md
# MAGIC Exit notebook if download successful

# COMMAND ----------

def cleanup_folder(folder_path):
    """
    Clean up folder by removing non-data files.
    
    Args:
        folder_path: Path to folder to clean
    """
    for f in dbutils.fs.ls(folder_path):
        if not f.name.startswith('part-00'):
            if not f.path.startswith('dbfs:/Volumes'):
                raise Exception(f"unexpected path, {f} throwing exception for safety")
            dbutils.fs.rm(f.path)

def save_sensor_data(spark_df, folder_path, partitions=100, cleanup=True, timestamp=False):
    """
    Save data to parquet format with cleanup.
    
    Args:
        spark_df: Spark DataFrame to save
        folder_path: Destination folder path
        partitions: Number of partitions for output
        cleanup: Whether to cleanup metadata files
        timestamp: Whether data is timestamped, order by timestamp
    """
    if timestamp:
        spark_df.orderBy('timestamp').repartition(partitions).write.mode('overwrite').format('parquet').save(folder_path)
    else:
        spark_df.repartition(partitions).write.mode('overwrite').format('parquet').save(folder_path)

    if cleanup:
        cleanup_folder(folder_path)

# COMMAND ----------

from data_generation import *

# COMMAND ----------

from pathlib import Path

BASE_CSV_PATH = Path("./platform_csvs")
PUBSEC_PLATFORM_DATA = BASE_CSV_PATH / "PdM_Platform_Data - pubsec_platform_data.csv"
NAVY_PLATFORM_DATA = BASE_CSV_PATH / "PdM_Platform_Data - navy_platform_data.csv"

if demo_type == 'navy':
  path = NAVY_PLATFORM_DATA
elif demo_type == 'pubsec':
  path = PUBSEC_PLATFORM_DATA
else:
  raise Exception("Invalid demo type")

platform_meta = get_platform_meta(spark, path=path)


# COMMAND ----------

# Generate sensor data using the dedicated sensor data generator
print("Starting sensor data generation...")
print_generation_summary()

base_folder = folder
TURBINE_PER_PLATFORM = 4
REALTIME_TURBINE_COUNT = TURBINE_PER_PLATFORM * platform_meta.select('designator').distinct().count()

# Generate historical data
print("Generating historical sensor data...")
historical_df = generate_historical_sensor_data(spark)
folder_historical = base_folder + '/historical_sensor_data'
save_sensor_data(historical_df, folder_historical, partitions=100)
print(f"Historical data saved to: {folder_historical}")

# Generate real-time data  
print("Generating real-time sensor data...")
realtime_df = generate_realtime_sensor_data(spark, turbine_count=REALTIME_TURBINE_COUNT)
folder_realtime = base_folder + '/incoming_data'
# Drop 'damaged' column for production pipeline (keep abnormal_sensor for validation)
realtime_df_clean = realtime_df.drop('damaged') if 'damaged' in realtime_df.columns else realtime_df
save_sensor_data(realtime_df_clean, folder_realtime, partitions=20)
print(f"Real-time data saved to: {folder_realtime}")

# # Set folder references for downstream processing
# folder_sensor = folder + '/incoming_data'
# folder_historical = folder + '/historical_sensor_data'

# Note: Cleanup is now handled automatically by the sensor_data_generator module

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assign RealTime Turbine IDs to Platforms

# COMMAND ----------

turbine_ids = realtime_df.select('turbine_id').distinct()
platform_count = platform_meta.select('designator').distinct().count()

platform_meta = map_platform_to_turbine(
  platform_meta=platform_meta.repartition(1),
  turbine_ids=turbine_ids.repartition(1),
  platform_count=platform_count
  )

save_sensor_data(platform_meta, folder+f"/meta_{demo_type}", partitions=5, cleanup=True, timestamp=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Inventory Data

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/5/52/EERE_illust_large_turbine.gif">

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate parts data for Navy
# MAGIC Generate parts data for Navy GTE with Navy based logistics centers. 
# MAGIC
# MAGIC Sensors B and D are lower level maintenance reapirs, so assosciate lower impact parts
# MAGIC Sensor F is a high level maintenance item, so only assosciate large repair items
# MAGIC
# MAGIC
# MAGIC These LRUs are designed for ease of maintenance and quick replacement. Here's a more detailed breakdown:
# MAGIC
# MAGIC ### Combustor Section
# MAGIC 1. **Combustor Liners**: Protects the surrounding structure from the high temperatures of combustion.
# MAGIC 2. **Fuel Nozzles**: Delivers fuel into the combustor in a precise pattern for efficient combustion. **USE**
# MAGIC 3. **Igniters**: Initiates combustion in the turbine.
# MAGIC
# MAGIC ### Turbine Section
# MAGIC 1. **Turbine Blades**: Converts the high-energy gas flow into rotational energy.
# MAGIC 2. **Turbine Vanes**: Directs the flow of gases onto the turbine blades. **USE - HIGH IMPACT**
# MAGIC 3. **Turbine Disk**: Holds the blades and connects to the rotor shaft.
# MAGIC
# MAGIC ### Compressor Section
# MAGIC 1. **Compressor Blades**: Compresses the incoming air before it enters the combustor.
# MAGIC 2. **Compressor Vanes**: Directs the air at the correct angle into the compressor blades. **USE - HIGH IMPACT**
# MAGIC 3. **Compressor Rotor Assembly**: The rotating part of the compressor, holding blades and disks.
# MAGIC
# MAGIC ### Bearings and Seals
# MAGIC 1. **Main Bearings**: Support the rotor and allow it to rotate smoothly.
# MAGIC 2. **Thrust Bearings**: Absorb axial forces generated by the turbine.
# MAGIC 3. **Seals**: Prevent leakage of air or fluids between different sections of the engine. **USE**
# MAGIC
# MAGIC ### Control and Monitoring
# MAGIC 1. **Electronic Control Units (ECU)**: Governs the turbine's operating parameters. **USE**
# MAGIC 2. **Sensors and Transducers**: Measure various parameters like temperature, pressure, and vibration. **USE**
# MAGIC 3. **Actuators**: Mechanical devices controlled by the ECU to adjust fuel flow, vane positions, etc.
# MAGIC
# MAGIC ### Auxiliary Systems
# MAGIC 1. **Fuel Pumps**: Supplies fuel to the combustion chamber. **USE**
# MAGIC 2. **Oil Pumps**: Circulate oil for lubrication and cooling. 
# MAGIC 3. **Filters (Oil and Fuel)**: Remove contaminants from oil and fuel. **USE**
# MAGIC 4. **Cooling Fans and Heat Exchangers**: Maintain operational temperatures.
# MAGIC
# MAGIC ### Gearbox (if applicable)
# MAGIC 1. **Gears and Shafts**: Transfers power from the turbine to the driven equipment.
# MAGIC 2. **Clutches**: Engage or disengage the drive connection.
# MAGIC
# MAGIC ### Exhaust and Intake
# MAGIC 1. **Exhaust Ducts**: Channel exhaust gases away from the turbine.
# MAGIC 2. **Silencers**: Reduce noise from the exhaust.
# MAGIC 3. **Air Inlet Filters**: Remove particulates from the intake air to protect the compressor blades.
# MAGIC
# MAGIC ### Miscellaneous
# MAGIC 1. **Valves (Fuel, Oil, Air)**: Control the flow of various fluids and gases in the system. **USE**
# MAGIC 2. **Couplings**: Connects the turbine shaft to the driven equipment or load.
# MAGIC
# MAGIC Each of these LRUs is a critical component of the LM2500 gas turbine and is designed for easy removal and replacement, facilitating efficient maintenance and reducing downtime. The specifics of these components, such as dimensions, materials, and exact design, would be detailed in the technical manuals and maintenance guides provided by the manufacturer, General Electric.
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Stock Information
# MAGIC Supply chain information for Navy Supply facilities. FLC is Fleet Logistics Center and are located throughout the US. Each location has a random number of each part generated for simulated stock information. This is used in downstream calculations for Supply Chain optimization 

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Stock information - NAVY

# COMMAND ----------

if demo_type == 'navy':
  path = './platform_csvs/PdM_Platform_Data - navy_stock_location_data.csv'
elif demo_type == 'pubsec':
  path = './platform_csvs/PdM_Platform_Data - pubsec_stock_location_data.csv'
else:
  raise Exception("Invalid demo type")

parts_meta, inventory = get_parts_and_stock(spark, path)

folder_parts = folder+f'/parts_{demo_type}'
save_sensor_data(inventory, folder_parts)

parts_meta.write.mode("overwrite").saveAsTable(f"parts_meta")

# COMMAND ----------

# # Read in the stock location data from local csv
# stock_data = pd.read_csv('./platform_csvs/PdM_Platform_Data - navy_stock_location_data.csv')

# #For each supply location, we'll generate supply chain parts
# # Low failure items + high failure items

# part_categories = [{'name': 'Vane - Turbine'}, {'name': 'Blade - Turbine'}, {'name': 'Fuel Nozzle'}, {'name': 'Seal'}, {'name': 'controller card #1 - ECU'}, {'name': 'controller card #2 - ECU'}, {'name': 'Pump - Fuel'}, {'name': 'Filter - Fuel / Oil'}, {'name': 'Valve - Fuel / Oil'}]

# # list to check against high impact vs low impact
# high_impact_parts = ['Vane - Turbine', 'Blade - Turbine']
# #sensors = get_sensor_columns(folder_sensor, spark)

# # Get sensor categorization from the sensor data generator
# sensor_categories = get_sensor_impact_categories()

# parts = []
# for p in part_categories:
#   # Associate parts with appropriate sensors based on maintenance impact level
#   nsn = faker.ean(length=8)
#   for location in stock_data['stock_location']:
#     part = {}
#     part['NSN'] = nsn
#     part['type'] = p['name']
#     part['width'] = rd.randint(100,2000)
#     part['height'] = rd.randint(100,2000)
#     part['weight'] = rd.randint(100,20000)
#     part['stock_available'] = rd.randint(0, 10)
#     part['stock_location'] =  location
#     part['production_time'] = rd.randint(0, 5)
#     #part['approvisioning_estimated_days'] = rd.randint(30,360)
#     if p['name'] in high_impact_parts:
#       part['sensors'] = sensor_categories['high_impact_sensors']
#     else:
#       # Parts can be associated with moderate or low impact sensors
#       part['sensors'] = random.sample(sensor_categories['all_non_high_impact'], rd.randint(1,2))
#     parts.append(part)

# # Join synthetic parts data with stock locations
# pubsec_stock_joined = pd.DataFrame(parts).merge(stock_data, on='stock_location', suffixes=('', '_pubsec'))

# # Create spark df, write to volume 
# df = spark.createDataFrame(pubsec_stock_joined)


# # df.write.mode('overwrite').format('json').save(folder_parts)
# # cleanup(folder_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Stock information - PUBSEC

# COMMAND ----------

# # Read in the stock location data from local csv
# stock_data = pd.read_csv('./platform_csvs/PdM_Platform_Data - pubsec_stock_location_data.csv')

# #For each supply location, we'll generate supply chain parts
# # Low failure items + high failure items

# part_categories = [{'name': 'Vane - Turbine'}, {'name': 'Blade - Turbine'}, {'name': 'Fuel Nozzle'}, {'name': 'Seal'}, {'name': 'controller card #1 - ECU'}, {'name': 'controller card #2 - ECU'}, {'name': 'Pump - Fuel'}, {'name': 'Filter - Fuel / Oil'}, {'name': 'Valve - Fuel / Oil'}]

# # list to check against high impact vs low impact
# high_impact_parts = ['Vane - Turbine', 'Blade - Turbine']
# # sensors = get_sensor_columns(folder_sensor, spark)

# # Get sensor categorization from the sensor data generator
# sensor_categories = get_sensor_impact_categories()

# parts = []
# for p in part_categories:
#   # Associate parts with appropriate sensors based on maintenance impact level
#   nsn = faker.ean(length=8)
#   for location in stock_data['stock_location']:
#     part = {}
#     part['NSN'] = nsn
#     part['type'] = p['name']
#     part['width'] = rd.randint(100,2000)
#     part['height'] = rd.randint(100,2000)
#     part['weight'] = rd.randint(100,20000)
#     part['stock_available'] = rd.randint(0, 10)
#     part['stock_location'] =  location
#     part['production_time'] = rd.randint(0, 5)
#     #part['approvisioning_estimated_days'] = rd.randint(30,360)
#     if p['name'] in high_impact_parts:
#       part['sensors'] = sensor_categories['high_impact_sensors']
#     else:
#       # Parts can be associated with moderate or low impact sensors
#       part['sensors'] = random.sample(sensor_categories['all_non_high_impact'], rd.randint(1,2))
#     parts.append(part)

# # Join synthetic parts data with stock locations
# pubsec_stock_joined = pd.DataFrame(parts).merge(stock_data, on='stock_location', suffixes=('', '_pubsec'))

# # Create spark df, write to volume 
# df = spark.createDataFrame(pubsec_stock_joined)
# folder_parts = folder+'/parts_pub'

# save_sensor_data(df, folder_parts)

# # df.write.mode('overwrite').format('json').save(folder_parts)
# # cleanup(folder_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Ship Meta Data 
# MAGIC Contains Ship name, homeport

# COMMAND ----------

# from pyspark.sql.functions import lit
# from pyspark.sql.functions import monotonically_increasing_id, floor, lit, hash, abs
# import pandas as pd

# file_location = "./platform_csvs/PdM_Platform_Data - navy_platform_data.csv"
# pandas_df = pd.read_csv(file_location)
# ships = spark.createDataFrame(pandas_df)

# ships = ships.withColumn("join_key", monotonically_increasing_id())

# # Get total number of ships for Turbine ID assignment
# num_ships = ships.select('designator').count()
# status = (spark.read.format('json').load(folder_status)
#           .drop('end_time')
#           .drop('start_time')
#           .drop('abnormal_sensor')
#           .withColumn('model', lit('LM2500'))
#           .withColumn('join_key', monotonically_increasing_id() % num_ships)
#           )

# # Assign turbine ID to ship metadata
# ship_meta = ships.join(status, 'join_key').drop('join_key').withColumn('designator_id', abs(hash('designator')))

# folder_ship = folder+'/ship_meta'
# ship_meta.write.mode('overwrite').format('json').save(folder_ship)
# cleanup(folder_ship)


# COMMAND ----------

# MAGIC %md
# MAGIC ## PUBSEC AMTRAK Metadata

# COMMAND ----------

# from pyspark.sql.functions import lit
# from pyspark.sql.functions import monotonically_increasing_id, floor, lit

# file_location = "./platform_csvs/PdM_Platform_Data - pubsec_platform_data.csv"
# pandas_df = pd.read_csv(file_location)
# ships = spark.createDataFrame(pandas_df)

# ships = ships.withColumn("join_key", monotonically_increasing_id())

# folder_status = folder+'/historical_turbine_status'
# # Get total number of ships for Turbine ID assignment
# num_ships = ships.select('designator').count()
# status = (spark.read.format('json').load(folder_status)
#           .drop('end_time')
#           .drop('start_time')
#           .drop('abnormal_sensor')
#           .withColumn('model', lit('LM2500'))
#           .withColumn('join_key', monotonically_increasing_id() % num_ships)
#           )
# # # Assign turbine ID to ship metadata
# ship_meta = ships.join(status, 'join_key').drop('join_key').withColumn('designator_id', abs(hash('designator')))

# folder_platform = folder+'/platform_meta'
# ship_meta.write.mode('overwrite').format('json').save(folder_platform)
# cleanup(folder_platform)
