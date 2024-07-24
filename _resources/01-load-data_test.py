# Databricks notebook source
# MAGIC %pip install git+https://github.com/QuentinAmbard/mandrova faker databricks-sdk==0.17.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

db = dbName = schema = 'test'
volume_name = 'navy_test'

# COMMAND ----------

# MAGIC %run ./00-global-setup-v2

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
folder = f"/Volumes/{catalog}/{db}/{volume_name}"

data_exists = False
try:
  dbutils.fs.ls(folder)
  dbutils.fs.ls(folder+"/historical_turbine_status")
  dbutils.fs.ls(folder+"/parts")
  dbutils.fs.ls(folder+"/turbine")
  dbutils.fs.ls(folder+"/incoming_data")
  dbutils.fs.ls(folder+"/ship_meta")
  data_exists = True
  print("data already exists")
except Exception as e:
  print(f"folder doesn't exists, generating the data...")

def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# COMMAND ----------

data_downloaded = False 

# if not data_exists:
#     try:
#         DBDemos.download_file_from_git(folder+'/historical_turbine_status', "awhahn07", "dbdemos-fed-datasets", "/navy_pdm_data/historical_turbine_status")
#         DBDemos.download_file_from_git(folder+'/parts', "awhahn07", "dbdemos-fed-datasets", "/navy_pdm_data/parts")
#         DBDemos.download_file_from_git(folder+'/turbine', "awhahn07", "dbdemos-fed-datasets", "/navy_pdm_data/turbine")
#         DBDemos.download_file_from_git(folder+'/incoming_data', "awhahn07", "dbdemos-fed-datasets", "/navy_pdm_data/incoming_data")
#         DBDemos.download_file_from_git(folder+'/ship_meta', "awhahn07", "dbdemos-fed-datasets", "/navy_pdm_data/ship_meta")
#         data_downloaded = True
#     except Exception as e: 
#         print(f"Error trying to download the file from the repo: {str(e)}. Will generate the data instead...")   

# COMMAND ----------

#As we need a model in the DLT pipeline and the model depends of the DLT pipeline too, let's build an empty one.
#This wouldn't make sense in a real-world system where we'd have 2 jobs / pipeline (1 for ingestion, and 1 to build the model / run inferences)
import random
import mlflow
from  mlflow.models.signature import ModelSignature

# define a custom model randomly flagging 10% of sensor for the demo init (it'll be replace with proper model on the training part.)
class MaintenanceEmptyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        import random
        sensors = ['sensor_F', 'sensor_D', 'sensor_B']  # List of sensors
        return model_input['avg_energy'].apply(lambda x: 'ok' if random.random() < 0.9 else random.choice(sensors))
 
#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')

# TODO Change Model Name
model_name = "navy_turbine_maintenance" #model_name = "dbdemos_turbine_maintenance"

#Only register empty model if model doesn't exist yet
client = mlflow.tracking.MlflowClient()
try:
  latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print("Model doesn't exist - saving an empty one")
        # setup the experiment folder
        DBDemos.init_experiment_for_batch("lakehouse-navy-PdM", "navy_turbine_maintenance")
        # save the model
        churn_model = MaintenanceEmptyModel()
        import pandas as pd
        
        signature = ModelSignature.from_dict({'inputs': '[{"name": "turbine_id", "type": "string"}, {"name": "hourly_timestamp", "type": "datetime"}, {"name": "avg_energy", "type": "double"}, {"name": "std_sensor_A", "type": "double"}, {"name": "std_sensor_B", "type": "double"}, {"name": "std_sensor_C", "type": "double"}, {"name": "std_sensor_D", "type": "double"}, {"name": "std_sensor_E", "type": "double"}, {"name": "std_sensor_F", "type": "double"}, {"name": "percentiles_sensor_A", "type": "string"}, {"name": "percentiles_sensor_B", "type": "string"}, {"name": "percentiles_sensor_C", "type": "string"}, {"name": "percentiles_sensor_D", "type": "string"}, {"name": "percentiles_sensor_E", "type": "string"}, {"name": "percentiles_sensor_F", "type": "string"}]',
'outputs': '[{"type": "tensor", "tensor-spec": {"dtype": "object", "shape": [-1]}}]'})
        
        with mlflow.start_run() as run:
            model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=churn_model, signature=signature, pip_requirements=['scikit-learn==1.1.1', 'mlflow==2.4.0'])

        #Register & move the model in production
        model_registered = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=model_registered.version)
    else:
        print(f"ERROR: couldn't access model for unknown reason - DLT pipeline will likely fail as model isn't available: {e}")

# COMMAND ----------

if data_downloaded:
    dbutils.notebook.exit(f"Data Downloaded to {folder}")

# COMMAND ----------

from mandrova.data_generator import SensorDataGenerator as sdg
import numpy as np
import pandas as pd
import random
import time
import uuid
import pyspark.sql.functions as F


def generate_sensor_data(turbine_id, sensor_conf, faulty = False, sample_size = 1000, display_graph = True, noise = 2, delta = -3):
  dg = sdg()
  rd = random.Random()
  rd.seed(turbine_id)
  dg.seed(turbine_id)
  sigma = sensor_conf['sigma']
  #Faulty, change the sigma with random value
  if faulty:
    sigma *= rd.randint(8,20)/10
    
  dg.generation_input.add_option(sensor_names="normal", distribution="normal", mu=0, sigma = sigma)
  dg.generation_input.add_option(sensor_names="sin", eq=f"2*exp(sin(t))+{delta}", initial={"t":0}, step={"t":sensor_conf['sin_step']})
  dg.generate(sample_size)
  sensor_name = "sensor_"+ sensor_conf['name']
  dg.sum(sensors=["normal", "sin"], save_to=sensor_name)
  max_value = dg.data[sensor_name].max()
  min_value = dg.data[sensor_name].min()
  if faulty:
    n_outliers = int(sample_size*0.15)
    outliers = np.random.uniform(-max_value*rd.randint(2,3), max_value*rd.randint(2,3), n_outliers)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
    dg.inject(value=outliers, sensor=sensor_name, index=indicies)

  n_outliers = int(sample_size*0.01)
  outliers = np.random.uniform(min_value*noise, max_value*noise, n_outliers)
  indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
  dg.inject(value=outliers, sensor=sensor_name, index=indicies)
  
  if display_graph:
    dg.plot_data(sensors=[sensor_name])
  return dg.data[sensor_name]

# COMMAND ----------

sensors = [{"name": "A", "sin_step": 0, "sigma": 1},
           {"name": "B", "sin_step": 0, "sigma": 2},
           {"name": "C", "sin_step": 0, "sigma": 3},
           {"name": "D", "sin_step": 0.1, "sigma": 1.5},
           {"name": "E", "sin_step": 0.01, "sigma": 2},
           {"name": "F", "sin_step": 0.2, "sigma": 1}]
current_time = int(time.time()) - 3600*30

#Sec between 2 metrics
frequency_sec = 10
#X points per turbine (1 point per frequency_sec second)
sample_size = 2125
training_turbine_count = 512
dfs = []

# COMMAND ----------

def generate_turbine_data(turbine, turbine_count):
  rd = random.Random()
  rd.seed(turbine)
  damaged = turbine > turbine_count*0.7
  if turbine % 10 == 0:
    print(f"generating turbine {turbine} - damage: {damaged}")
  df = pd.DataFrame()
  damaged_sensors = []
  rd.shuffle(sensors)

  # Increase or decrease failure frequency here for realism, i.e. sensor_F lower frequency as higher impact failure
  # 5% Sensor F failure
  # 40% Sensor D Failure
  # 55% Sensor B Fial
  if damaged:
    # 5% Fail Sensor F
    if turbine % 20 == 0:
      damaged_sensors.append('sensor_F')
    # 33% Fail Sensor D
    elif turbine % 3 == 0: 
      damaged_sensors.append('sensor_D')
    # 62% Fail Sensor B
    else:
      damaged_sensors.append('sensor_B')

  for s in sensors:
    #30% change to have 1 sensor being damaged
    #Only 1 sensor can send damaged metrics at a time to simplify the model. A C and E won't be damaged for simplification
    # ORIGINAL SENSOR GEN CODE
    # if damaged and len(damaged_sensors) == 0 and s['name'] not in ["A", "C", "E"]:
    #   damaged_sensor = rd.randint(1,10) > 5
    # else:
    #   damaged_sensor = False
    # if damaged_sensor:
    #   damaged_sensors.append('sensor_'+s['name'])
    
    # NEW SENSOR GEN CODE
    if len(damaged_sensors) > 0 and s in damaged_sensors:
      damaged_sensor = True
    else:
      damaged_sensor = False
    
    plot = turbine == 0
    df['sensor_'+s['name']] = generate_sensor_data(turbine, s, damaged_sensor, sample_size, plot)

  dg = sdg()
  #Damaged turbine will produce less
  factor = 50 if damaged else 30
  energy = dg.generation_input.add_option(sensor_names="energy", eq="x", initial={"x":0}, step={"x":np.absolute(np.random.randn(sample_size).cumsum()/factor)})
  dg.generate(sample_size, seed=rd.uniform(0,10000))
  #Add some null values in some timeseries to get expectation metrics
  if damaged and rd.randint(0,9) >7:
    n_nulls = int(sample_size*0.005)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_nulls))
    dg.inject(value=None, sensor="energy", index=indicies)

  if plot:
    dg.plot_data()
  df['energy'] = dg.data['energy']

  df.insert(0, 'timestamp', range(current_time, current_time + len(df)*frequency_sec, frequency_sec))
  df['turbine_id'] = str(uuid.UUID(int=rd.getrandbits(128)))
  #df['damaged'] = damaged
  df['abnormal_sensor'] = "ok" if len(damaged_sensors) == 0 else damaged_sensors[0]
  return df

from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf, col  

df_schema=spark.createDataFrame(generate_turbine_data(0, 1)) 

def generate_turbine(iterator: Iterator[pd.DataFrame], turbine_count) -> Iterator[pd.DataFrame]:
  for pdf in iterator:
    for i, row in pdf.iterrows():
      yield generate_turbine_data(row["id"], turbine_count)

spark_df = spark.range(0, training_turbine_count).repartition(int(training_turbine_count/10)).mapInPandas(lambda iterator: generate_turbine(iterator, training_turbine_count), schema=df_schema.schema)
spark_df = spark_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Ship Meta Data 
# MAGIC Contains Ship name, homeport

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id, floor, lit

#Read this from GIT

file_location = "/FileStore/tables/ahahn/US_ships_homeport.csv"
ships = spark.read.format("csv") \
  .option("inferSchema", "false") \
  .option("header", "true") \
  .option("sep", ",") \
  .load(file_location)
ships = ships.withColumn("join_key", monotonically_increasing_id())

# Get total number of ships for Turbine ID assignment
num_ships = ships.select('ship').count()


# COMMAND ----------

# MAGIC %md
# MAGIC # Generate Incoming Data (limited to number of ships * num tubrine / ship)
# MAGIC In this case, there are 51 ships and 4 turbines per ship, so 204 turbines

# COMMAND ----------

root_folder = folder
folder_sensor = root_folder+'/incoming_data'

# Total of 204 real time sensors
incoming_turbines = 512
#incoming_turbines = num_ships * 4

incoming_df = spark.range(0, incoming_turbines).repartition(int(incoming_turbines/10)).mapInPandas(lambda iterator: generate_turbine(iterator, incoming_turbines), schema=df_schema.schema)
incoming_df = incoming_df.cache()


incoming_df.drop('damaged').drop('abnormal_sensor').orderBy('timestamp').repartition(100).write.mode('overwrite').format('parquet').save(folder_sensor)

#Cleanup meta file to only keep names
def cleanup(folder):
  for f in dbutils.fs.ls(folder):
    if not f.name.startswith('part-00'):
      if not f.path.startswith('dbfs:/Volumes'):
        raise Exception(f"unexpected path, {f} throwing exception for safety")
      dbutils.fs.rm(f.path)
      
cleanup(folder_sensor)

# COMMAND ----------

# MAGIC %md
# MAGIC # Assosciate Ship Meta with incoming data turbine IDs

# COMMAND ----------

sensor = (incoming_df.drop('end_time')
          .drop('start_time')
          .drop('abnormal_sensor')
          .withColumn('model', lit('LM2500'))
          .withColumn('join_key', monotonically_increasing_id() % num_ships)
        )
# sensor = (spark.read.format('json').load(folder_sensor)
#           .drop('end_time')
#           .drop('start_time')
#           .drop('abnormal_sensor')
#           .withColumn('model', lit('LM2500'))
#           .withColumn('join_key', monotonically_increasing_id() % num_ships)
#           )

# Assign turbine ID to ship metadata
ship_meta = ships.join(sensor, 'join_key').drop('join_key')

folder_ship = root_folder+'/ship_meta'
ship_meta.write.mode('overwrite').format('json').save(folder_ship)
cleanup(folder_ship)

# ship_meta.select('turbine_id').distinct().count()

# COMMAND ----------

# root_folder = folder
# folder_sensor = root_folder+'/incoming_data'
# spark_df.drop('damaged').drop('abnormal_sensor').orderBy('timestamp').repartition(100).write.mode('overwrite').format('parquet').save(folder_sensor)

# #Cleanup meta file to only keep names
# def cleanup(folder):
#   for f in dbutils.fs.ls(folder):
#     if not f.name.startswith('part-00'):
#       if not f.path.startswith('dbfs:/Volumes'):
#         raise Exception(f"unexpected path, {f} throwing exception for safety")
#       dbutils.fs.rm(f.path)
      
# cleanup(folder_sensor)

# COMMAND ----------

from faker import Faker
from pyspark.sql.types import ArrayType, FloatType, StringType
import pyspark.sql.functions as F

Faker.seed(0)
faker = Faker()
fake_latlng = F.udf(lambda: list(faker.local_latlng(country_code = 'US')), ArrayType(StringType()))

# COMMAND ----------

rd = random.Random()
rd.seed(0)
folder = root_folder+'/turbine'
#TODO Update to erase erroneous fields
(spark_df.select('turbine_id').drop_duplicates()
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.col('fake_lat_long').getItem(0))
   .withColumn('long', F.col('fake_lat_long').getItem(1))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long')
 .orderBy(F.rand()).repartition(1).write.mode('overwrite').format('json').save(folder))

#Add some turbine with wrong data for expectations
# TODO Update for realistic data quality metrics
fake_null_uuid = F.udf(lambda: None if rd.randint(0,9) > 2 else str(uuid.uuid4()))
df_error = (spark_df.select('turbine_id').limit(30)
   .withColumn('turbine_id', fake_null_uuid())
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.lit("ERROR"))
   .withColumn('long', F.lit("ERROR"))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long').repartition(1).write.mode('append').format('json').save(folder))
cleanup(folder)

folder_status = root_folder+'/historical_turbine_status'
(spark_df.select('turbine_id', 'abnormal_sensor').drop_duplicates()
         .withColumn('start_time', (F.lit(current_time-1000)-F.rand()*2000).cast('int'))
         .withColumn('end_time', (F.lit(current_time+3600*24*30)+F.rand()*4000).cast('int'))
         .repartition(1).write.mode('overwrite').format('json').save(folder_status))
cleanup(folder_status)

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

# Replace state with FLCs and homeport, possbibly distance matrix
stock_locations = [
'FLC Jacksonville',
'FLC Norfolk',
'FLC Pearl Harbor',
'FLC Puget Sound',
'FLC San Diego'
]

#For each supply location, we'll generate supply chain parts
# Low failure items + high failure items

part_categories = [{'name': 'Vane - Turbine'}, {'name': 'Blade - Turbine'}, {'name': 'Fuel Nozzle'}, {'name': 'Seal'}, {'name': 'controller card #1 - ECU'}, {'name': 'controller card #2 - ECU'}, {'name': 'Pump - Fuel'}, {'name': 'Filter - Fuel / Oil'}, {'name': 'Valve - Fuel / Oil'}]

# list to check against high impact vs low impact
high_impact_parts = ['Vane - Turbine', 'Blade - Turbine']
sensors = [c for c in spark.read.parquet(folder_sensor).columns if "sensor" in c]

# TODO Can be changed if using more than sensors B, D, F
low_impact_sensors = [s for s in sensors if s != 'sensor_F']

parts = []
for p in part_categories:
  # Add a conditional in here to only assosciate high impact parts with sensor F failures
  nsn = faker.ean(length=8)
  for location in stock_locations:
    part = {}
    part['NSN'] = nsn
    part['type'] = p['name']
    part['width'] = rd.randint(100,2000)
    part['height'] = rd.randint(100,2000)
    part['weight'] = rd.randint(100,20000)
    part['stock_available'] = rd.randint(0, 10)
    part['stock_location'] =  location
    part['production_time'] = rd.randint(0, 5)
    #part['approvisioning_estimated_days'] = rd.randint(30,360)
    if p['name'] in high_impact_parts:
      part['sensors'] = ['sensor_F']
    else:
      #part['sensors'] = random.sample(low_impact_sensors, rd.randint(1,3))
      # Parts only assigned to 1 sensor
      part['sensors'] = random.sample(low_impact_sensors, rd.randint(1,2))
    parts.append(part)

df = spark.createDataFrame(parts)

folder_parts = root_folder+'/parts'
df.write.mode('overwrite').format('json').save(folder_parts)
cleanup(folder_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Ship Meta Data 
# MAGIC Contains Ship name, homeport

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id, floor, lit

file_location = "/FileStore/tables/ahahn/US_ships_homeport.csv"
ships = spark.read.format("csv") \
  .option("inferSchema", "false") \
  .option("header", "true") \
  .option("sep", ",") \
  .load(file_location)
ships = ships.withColumn("join_key", monotonically_increasing_id())

# Get total number of ships for Turbine ID assignment
num_ships = ships.select('ship').count()
status = (spark.read.format('json').load(folder_status)
          .drop('end_time')
          .drop('start_time')
          .drop('abnormal_sensor')
          .withColumn('model', lit('LM2500'))
          .withColumn('join_key', monotonically_increasing_id() % num_ships)
          )

# Assign turbine ID to ship metadata
ship_meta = ships.join(status, 'join_key').drop('join_key')

folder_ship = root_folder+'/ship_meta'
ship_meta.write.mode('overwrite').format('json').save(folder_ship)
cleanup(folder_ship)

