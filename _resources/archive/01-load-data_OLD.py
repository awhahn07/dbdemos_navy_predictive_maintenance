# Databricks notebook source
# MAGIC %pip install git+https://github.com/QuentinAmbard/mandrova faker

# COMMAND ----------

dbutils.widgets.text('root_folder', '/FileStore/tables/ahahn/predictive_maintenance', 'Root Folder')
root_folder = dbutils.widgets.get('root_folder')

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
# Update this to reflect Num_Ships * 4 = 204
turbine_count = 204
dfs = []

# COMMAND ----------

def generate_turbine_data(turbine):
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

df_schema=spark.createDataFrame(generate_turbine_data(0)) 

def generate_turbine(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
  for pdf in iterator:
    for i, row in pdf.iterrows():
      yield generate_turbine_data(row["id"])

spark_df = spark.range(0, turbine_count).repartition(int(turbine_count/10)).mapInPandas(generate_turbine, schema=df_schema.schema)
spark_df = spark_df.cache()

# COMMAND ----------

folder_sensor = root_folder+'/incoming_data'
spark_df.drop('damaged').drop('abnormal_sensor').orderBy('timestamp').repartition(100).write.mode('overwrite').format('json').save(folder_sensor)

#Cleanup meta file to only keep names
def cleanup(folder):
  for f in dbutils.fs.ls(folder):
    if not f.name.startswith('part-00'):
      if not f.path.startswith(f'dbfs:{root_folder}'):
        raise Exception(f"unexpected path, {f} throwing exception for safety")
      dbutils.fs.rm(f.path)
      
cleanup(folder_sensor)

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

# TODO update for ship metadata 

folder = root_folder+'/turbine'
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

# COMMAND ----------

# #see https://blog.enerpac.com/wind-turbine-maintenance-components-strategies-and-tools/
# #Get the list of states where our wind turbine are
# states = spark.read.json(folder).select('state').distinct().collect()
# states = [s['state'] for s in states]

# #For each state, we'll generate supply chain parts
# part_categories = [{'name': 'blade'}, {'name': 'Yaw drive'}, {'name': 'Brake'}, {'name': 'anemometer'}, {'name': 'controller card #1'}, {'name': 'controller card #2'}, {'name': 'Yaw motor'}, {'name': 'hydraulics'}, {'name': 'electronic guidance system'}]
# sensors = [c for c in spark.read.json(folder_sensor).columns if "sensor" in c]
# parts = []
# for p in part_categories:
#   for _ in range (1, rd.randint(30, 100)):
#     part = {}
#     part['EAN'] = None if rd.randint(0,100) > 95 else faker.ean(length=8)
#     part['type'] = p['name']
#     part['width'] = rd.randint(100,2000)
#     part['height'] = rd.randint(100,2000)
#     part['weight'] = rd.randint(100,20000)
#     part['stock_available'] = rd.randint(0, 5)
#     #part['stock_location'] =   random.sample(states, 1)[0]
#     part['production_time'] = rd.randint(0, 5)
#     part['approvisioning_estimated_days'] = rd.randint(30,360)
#     part['sensors'] = random.sample(sensors, rd.randint(1,3))
#     parts.append(part)
# df = spark.createDataFrame(parts)
# folder_parts = root_folder+'/parts'
# df.repartition(3).write.mode('overwrite').format('json').save(folder_parts)
# cleanup(folder_parts)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate parts data for Navy
# MAGIC Generate parts data for Navy GTE with Navy based logistics centers. 
# MAGIC
# MAGIC Sensors B and D are lower level maintenance reapirs, so assosciate lower impact parts
# MAGIC Sensor F is a high level maintenance item, so only assosciate large repair items
# MAGIC

# COMMAND ----------

# MAGIC %md
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stock Information
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
sensors = [c for c in spark.read.json(folder_sensor).columns if "sensor" in c]

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
    part['stock_available'] = rd.randint(0, 30)
    part['stock_location'] =  location
    part['production_time'] = rd.randint(0, 5)
    #part['approvisioning_estimated_days'] = rd.randint(30,360)
    if p in high_impact_parts:
      part['sensors'] = 'sensor_F'
    else:
      #part['sensors'] = random.sample(low_impact_sensors, rd.randint(1,3))
      # Parts only assigned to 1 sensor
      part['sensors'] = random.sample(low_impact_sensors, rd.randint(1,2))
    parts.append(part)

df = spark.createDataFrame(parts)

# root_folder = '/FileStore/tables/ahahn/predictive_maintenance'
folder_parts = root_folder+'/parts'
df.write.mode('overwrite').format('json').save(folder_parts)
cleanup(folder_parts)
