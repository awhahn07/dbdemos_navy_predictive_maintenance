# Databricks notebook source
# MAGIC %md
# MAGIC # Sensor Data Generator
# MAGIC This script contains all the sensor data generation functionality for the Navy Predictive Maintenance demo.
# MAGIC It generates realistic sensor data with configurable failure modes and distributions.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/QuentinAmbard/mandrova faker numpy==1.26.4 pandas==2.2.3
# MAGIC
# MAGIC # Restart the Python environment  
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ./00-global-setup-v2

# COMMAND ----------

reset_all_data = False
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
folder = f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

# Required imports for sensor data generation
from mandrova.data_generator import SensorDataGenerator as sdg
import numpy as np
import pandas as pd
import random
import time
import uuid
import pyspark.sql.functions as F
from typing import Iterator
from pyspark.sql.functions import pandas_udf, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensor Configuration
# MAGIC Define the 6 sensors with their characteristics and failure distributions

# COMMAND ----------

# Sensor configurations - each sensor has different behavior patterns
SENSORS = [
    {"name": "A", "sin_step": 0, "sigma": 1},       # Compressor related - moderate impact
    {"name": "B", "sin_step": 0, "sigma": 2},       # Existing - moderate impact  
    {"name": "C", "sin_step": 0, "sigma": 3},       # Cooling system - lower impact
    {"name": "D", "sin_step": 0.1, "sigma": 1.5},   # Existing - lower impact
    {"name": "E", "sin_step": 0.01, "sigma": 2},    # Fuel system - moderate impact
    {"name": "F", "sin_step": 0.2, "sigma": 1}      # High impact (turbine blades/vanes)
]

# Data generation parameters
DEFAULT_FREQUENCY_SEC = 10  # Seconds between 2 metrics
DEFAULT_SAMPLE_SIZE = 4250  # Data points per turbine (doubled for better ML training)
DEFAULT_TURBINE_COUNT = 400  # Total turbines for historical data (increased for more diversity)
REALTIME_TURBINE_COUNT = 50  # Fewer units for real-time simulation (50 units × 4 turbines = 200 turbines)

# Failure distribution percentages (realistic operational data)
FAILURE_DISTRIBUTIONS = {
    'sensor_C': 30,  # Cooling system - most frequent (0-29%)
    'sensor_D': 25,  # Routine maintenance (30-54%)  
    'sensor_A': 20,  # Compressor related (55-74%)
    'sensor_B': 15,  # Moderate impact (75-89%)
    'sensor_E': 7,   # Fuel system (90-96%)
    'sensor_F': 3    # High impact turbine blades/vanes (97-99%)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Sensor Generation Functions

# COMMAND ----------

def generate_sensor_data(turbine_id, sensor_conf, faulty=False, sample_size=1000, display_graph=True, noise=2, delta=-3):
    """
    Generate sensor data for a single sensor with optional fault injection.
    
    Args:
        turbine_id: Unique identifier for the turbine (used for seeding)
        sensor_conf: Sensor configuration dict with name, sin_step, sigma
        faulty: Whether to inject faults into the data
        sample_size: Number of data points to generate
        display_graph: Whether to plot the generated data
        noise: Noise level multiplier
        delta: Offset for the sine wave equation
        
    Returns:
        Pandas Series with generated sensor data
    """
    dg = sdg()
    rd = random.Random()
    rd.seed(turbine_id)
    dg.seed(turbine_id)
    sigma = sensor_conf['sigma']
    
    # Faulty sensor: increase sigma with random value
    if faulty:
        sigma *= rd.randint(8, 20) / 10
        
    dg.generation_input.add_option(sensor_names="normal", distribution="normal", mu=0, sigma=sigma)
    dg.generation_input.add_option(sensor_names="sin", eq=f"2*exp(sin(t))+{delta}", initial={"t": 0}, step={"t": sensor_conf['sin_step']})
    dg.generate(sample_size)
    sensor_name = "sensor_" + sensor_conf['name']
    dg.sum(sensors=["normal", "sin"], save_to=sensor_name)
    max_value = dg.data[sensor_name].max()
    min_value = dg.data[sensor_name].min()
    
    # Inject fault-specific outliers if faulty
    if faulty:
        n_outliers = int(sample_size * 0.15)
        outliers = np.random.uniform(-max_value * rd.randint(2, 3), max_value * rd.randint(2, 3), n_outliers)
        indices = np.sort(np.random.randint(0, sample_size - 1, n_outliers))
        dg.inject(value=outliers, sensor=sensor_name, index=indices)

    # Add normal operational noise (1% of data points)
    n_outliers = int(sample_size * 0.01)
    outliers = np.random.uniform(min_value * noise, max_value * noise, n_outliers)
    indices = np.sort(np.random.randint(0, sample_size - 1, n_outliers))
    dg.inject(value=outliers, sensor=sensor_name, index=indices)
    
    if display_graph:
        dg.plot_data(sensors=[sensor_name])
    return dg.data[sensor_name]


def determine_failed_sensor(turbine_id, failure_distributions=None):
    """
    Determine which sensor fails based on realistic failure distributions.
    
    Args:
        turbine_id: Turbine identifier for consistent random seeding
        failure_distributions: Dict of sensor failure percentages
        
    Returns:
        String name of failed sensor or None if no failure
    """
    if failure_distributions is None:
        failure_distributions = FAILURE_DISTRIBUTIONS
        
    failure_rand = turbine_id % 100
    cumulative = 0
    
    for sensor, percentage in failure_distributions.items():
        if failure_rand < cumulative + percentage:
            return sensor
        cumulative += percentage
    
    # Should not reach here if distributions sum to 100
    return None


def generate_turbine_data(turbine_id, sensors=None, sample_size=None, turbine_count=None, frequency_sec=None, base_time=None):
    """
    Generate complete turbine data including all sensors and energy readings.
    
    Args:
        turbine_id: Unique turbine identifier
        sensors: List of sensor configurations (uses SENSORS if None)
        sample_size: Number of data points to generate (uses DEFAULT_SAMPLE_SIZE if None)
        turbine_count: Total number of turbines (for damage probability calculation)
        frequency_sec: Time between measurements in seconds
        base_time: Base timestamp for data generation
        
    Returns:
        Pandas DataFrame with all sensor data, energy, and metadata
    """
    if sensors is None:
        sensors = SENSORS
    if sample_size is None:
        sample_size = DEFAULT_SAMPLE_SIZE
    if turbine_count is None:
        turbine_count = DEFAULT_TURBINE_COUNT
    if frequency_sec is None:
        frequency_sec = DEFAULT_FREQUENCY_SEC
    if base_time is None:
        base_time = int(time.time()) - 3600 * 30
        
    rd = random.Random()
    rd.seed(turbine_id)
    
    # Determine if turbine is damaged (70% of turbines can be damaged)
    damaged = turbine_id > turbine_count * 0.7
    
    if turbine_id % 10 == 0:
        print(f"generating turbine {turbine_id} - damage: {damaged}")
        
    df = pd.DataFrame()
    damaged_sensors = []
    rd.shuffle(sensors)

    # Determine which sensor fails if turbine is damaged
    if damaged:
        failed_sensor = determine_failed_sensor(turbine_id)
        if failed_sensor:
            damaged_sensors.append(failed_sensor)

    # Generate data for each sensor
    for s in sensors:
        sensor_name = 'sensor_' + s['name']
        # Check if this sensor should be damaged
        if len(damaged_sensors) > 0 and sensor_name in damaged_sensors:
            damaged_sensor = True
        else:
            damaged_sensor = False
        plot = turbine_id == 0  # Only plot for first turbine
        df[sensor_name] = generate_sensor_data(turbine_id, s, damaged_sensor, sample_size, plot)

    # Generate energy data
    dg = sdg()
    # Damaged turbine will produce less energy
    factor = 50 if damaged else 30
    dg.generation_input.add_option(sensor_names="energy", eq="x", initial={"x": 0}, 
                                  step={"x": np.absolute(np.random.randn(sample_size).cumsum() / factor)})
    dg.generate(sample_size, seed=rd.uniform(0, 10000))
    
    # Add some null values in energy timeseries for damaged turbines (data quality simulation)
    if damaged and rd.randint(0, 9) > 7:
        n_nulls = int(sample_size * 0.005)
        indices = np.sort(np.random.randint(0, sample_size - 1, n_nulls))
        dg.inject(value=None, sensor="energy", index=indices)

    if turbine_id == 0:  # Plot energy for first turbine
        dg.plot_data()
    df['energy'] = dg.data['energy']

    # Add metadata
    df.insert(0, 'timestamp', range(base_time, base_time + len(df) * frequency_sec, frequency_sec))
    df['turbine_id'] = str(uuid.UUID(int=rd.getrandbits(128)))
    df['abnormal_sensor'] = "ok" if len(damaged_sensors) == 0 else damaged_sensors[0]
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark DataFrame Generation Functions

# COMMAND ----------

def generate_turbine_batch(iterator: Iterator[pd.DataFrame], **kwargs) -> Iterator[pd.DataFrame]:
    """
    Pandas UDF function for generating turbine data in Spark.
    
    Args:
        iterator: Iterator of pandas DataFrames with turbine IDs
        **kwargs: Additional arguments passed to generate_turbine_data
        
    Yields:
        Generated turbine data DataFrames
    """
    for pdf in iterator:
        for i, row in pdf.iterrows():
            yield generate_turbine_data(row["id"], **kwargs)


def generate_realtime_turbine_batch(iterator: Iterator[pd.DataFrame], **kwargs) -> Iterator[pd.DataFrame]:
    """
    Pandas UDF function for generating real-time turbine data in Spark.
    Uses offset turbine IDs to get different failure patterns.
    
    Args:
        iterator: Iterator of pandas DataFrames with turbine IDs
        **kwargs: Additional arguments passed to generate_turbine_data
        
    Yields:
        Generated real-time turbine data DataFrames
    """
    for pdf in iterator:
        for i, row in pdf.iterrows():
            # Generate with turbine ID offset to get different failure patterns
            yield generate_turbine_data(row["id"] + 1000, **kwargs)


def generate_historical_sensor_data(spark, turbine_count=None, sample_size=None, repartition_factor=10):
    """
    Generate historical sensor data using Spark for parallel processing.
    
    Args:
        spark: Spark session
        turbine_count: Number of turbines to generate (uses DEFAULT_TURBINE_COUNT if None)
        sample_size: Data points per turbine (uses DEFAULT_SAMPLE_SIZE if None)
        repartition_factor: Factor to determine number of partitions
        
    Returns:
        Spark DataFrame with historical sensor data
    """
    if turbine_count is None:
        turbine_count = DEFAULT_TURBINE_COUNT
    if sample_size is None:
        sample_size = DEFAULT_SAMPLE_SIZE
        
    # Get schema from a sample
    df_schema = spark.createDataFrame(generate_turbine_data(0, sample_size=sample_size, turbine_count=turbine_count))
    
    # Generate data using Spark parallel processing
    spark_df = spark.range(0, turbine_count).repartition(int(turbine_count / repartition_factor)).mapInPandas(
        lambda iterator: generate_turbine_batch(iterator, sample_size=sample_size, turbine_count=turbine_count), 
        schema=df_schema.schema
    )
    
    return spark_df


def generate_realtime_sensor_data(spark, turbine_count=None, sample_size=None, repartition_factor=5):
    """
    Generate real-time sensor data using Spark for parallel processing.
    
    Args:
        spark: Spark session
        turbine_count: Number of real-time turbines to generate (uses REALTIME_TURBINE_COUNT if None)
        sample_size: Data points per turbine (smaller for real-time batches)
        repartition_factor: Factor to determine number of partitions
        
    Returns:
        Spark DataFrame with real-time sensor data
    """
    if turbine_count is None:
        turbine_count = REALTIME_TURBINE_COUNT
    if sample_size is None:
        sample_size = 200  # Smaller batches for real-time inference
        
    # Use current time for real-time data
    current_time = int(time.time())
    
    # Get schema from a sample
    df_schema = spark.createDataFrame(generate_turbine_data(0, sample_size=sample_size, 
                                                           turbine_count=turbine_count, base_time=current_time))
    
    # Generate real-time data using Spark parallel processing
    spark_df = spark.range(0, turbine_count).repartition(int(turbine_count / repartition_factor)).mapInPandas(
        lambda iterator: generate_realtime_turbine_batch(iterator, sample_size=sample_size, 
                                                        turbine_count=turbine_count, base_time=current_time), 
        schema=df_schema.schema
    )
    
    return spark_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Persistence Functions

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


def save_sensor_data(spark_df, folder_path, partitions=100, cleanup=True):
    """
    Save sensor data to parquet format with cleanup.
    
    Args:
        spark_df: Spark DataFrame to save
        folder_path: Destination folder path
        partitions: Number of partitions for output
        cleanup: Whether to cleanup metadata files
    """
    spark_df.orderBy('timestamp').repartition(partitions).write.mode('overwrite').format('parquet').save(folder_path)
    
    if cleanup:
        cleanup_folder(folder_path)


def generate_and_save_all_sensor_data(spark, base_folder):
    """
    Generate and save both historical and real-time sensor data.
    
    Args:
        spark: Spark session
        base_folder: Base folder path for saving data
        
    Returns:
        Tuple of (historical_df, realtime_df) Spark DataFrames
    """
    # Generate historical data
    print("Generating historical sensor data...")
    historical_df = generate_historical_sensor_data(spark)
    folder_historical = base_folder + '/historical_sensor_data'
    save_sensor_data(historical_df, folder_historical, partitions=100)
    print(f"Historical data saved to: {folder_historical}")
    
    # Generate real-time data  
    print("Generating real-time sensor data...")
    realtime_df = generate_realtime_sensor_data(spark)
    folder_realtime = base_folder + '/incoming_data'
    # Drop 'damaged' column for production pipeline (keep abnormal_sensor for validation)
    realtime_df_clean = realtime_df.drop('damaged') if 'damaged' in realtime_df.columns else realtime_df
    save_sensor_data(realtime_df_clean, folder_realtime, partitions=20)
    print(f"Real-time data saved to: {folder_realtime}")
    
    return historical_df, realtime_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def get_sensor_columns(folder_path, spark):
    """
    Get list of sensor columns from existing sensor data.
    
    Args:
        folder_path: Path to sensor data folder
        spark: Spark session
        
    Returns:
        List of sensor column names
    """
    return [c for c in spark.read.parquet(folder_path).columns if "sensor" in c]


def get_sensor_impact_categories():
    """
    Get sensor categorization by maintenance impact level.
    
    Returns:
        Dict with high_impact_sensors, moderate_impact_sensors, low_impact_sensors, and all_non_high_impact lists
    """
    return {
        'high_impact_sensors': ['sensor_F'],  # Major overhaul items
        'moderate_impact_sensors': ['sensor_A', 'sensor_B', 'sensor_E'],  # Component wear items  
        'low_impact_sensors': ['sensor_C', 'sensor_D'],  # Routine maintenance items
        'all_non_high_impact': ['sensor_A', 'sensor_B', 'sensor_E', 'sensor_C', 'sensor_D']
    }


def print_generation_summary():
    """Print summary of sensor data generation configuration."""
    print("=" * 60)
    print("SENSOR DATA GENERATION CONFIGURATION")
    print("=" * 60)
    print(f"Sensors configured: {len(SENSORS)} sensors (A, B, C, D, E, F)")
    print(f"Historical turbines: {DEFAULT_TURBINE_COUNT}")
    print(f"Real-time turbines: {REALTIME_TURBINE_COUNT}")
    print(f"Sample size per turbine: {DEFAULT_SAMPLE_SIZE}")
    print(f"Total historical samples: ~{DEFAULT_TURBINE_COUNT * DEFAULT_SAMPLE_SIZE:,}")
    print(f"Frequency: {DEFAULT_FREQUENCY_SEC} seconds between measurements")
    print("\nFailure Distributions:")
    for sensor, percentage in FAILURE_DISTRIBUTIONS.items():
        print(f"  {sensor}: {percentage}%")
    print("=" * 60)

# COMMAND ----------

historical_df, realtime_df = generate_and_save_all_sensor_data(spark, folder)

# COMMAND ----------

display(historical_df.limit(10))

# COMMAND ----------

display(realtime_df.limit(10))

# COMMAND ----------

historical_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{db}.historical_sensor_data")
realtime_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{db}.realtime_sensor_data")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE historical_sensor_hourly AS
# MAGIC SELECT turbine_id,
# MAGIC       date_trunc('hour', from_unixtime(timestamp)) AS hourly_timestamp, 
# MAGIC       avg(energy)          as avg_energy,
# MAGIC       stddev_pop(sensor_A) as std_sensor_A,
# MAGIC       stddev_pop(sensor_B) as std_sensor_B,
# MAGIC       stddev_pop(sensor_C) as std_sensor_C,
# MAGIC       stddev_pop(sensor_D) as std_sensor_D,
# MAGIC       stddev_pop(sensor_E) as std_sensor_E,
# MAGIC       stddev_pop(sensor_F) as std_sensor_F,
# MAGIC       percentile_approx(sensor_A, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_A,
# MAGIC       percentile_approx(sensor_B, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_B,
# MAGIC       percentile_approx(sensor_C, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_C,
# MAGIC       percentile_approx(sensor_D, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_D,
# MAGIC       percentile_approx(sensor_E, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_E,
# MAGIC       percentile_approx(sensor_F, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_F,
# MAGIC       abnormal_sensor
# MAGIC   FROM historical_sensor_data GROUP BY hourly_timestamp, turbine_id, abnormal_sensor

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE realtime_sensor_hourly AS
# MAGIC SELECT turbine_id,
# MAGIC       date_trunc('hour', from_unixtime(timestamp)) AS hourly_timestamp, 
# MAGIC       avg(energy)          as avg_energy,
# MAGIC       stddev_pop(sensor_A) as std_sensor_A,
# MAGIC       stddev_pop(sensor_B) as std_sensor_B,
# MAGIC       stddev_pop(sensor_C) as std_sensor_C,
# MAGIC       stddev_pop(sensor_D) as std_sensor_D,
# MAGIC       stddev_pop(sensor_E) as std_sensor_E,
# MAGIC       stddev_pop(sensor_F) as std_sensor_F,
# MAGIC       percentile_approx(sensor_A, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_A,
# MAGIC       percentile_approx(sensor_B, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_B,
# MAGIC       percentile_approx(sensor_C, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_C,
# MAGIC       percentile_approx(sensor_D, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_D,
# MAGIC       percentile_approx(sensor_E, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_E,
# MAGIC       percentile_approx(sensor_F, array(0.1, 0.3, 0.6, 0.8, 0.95)) as percentiles_sensor_F,
# MAGIC       abnormal_sensor
# MAGIC   FROM realtime_sensor_data GROUP BY hourly_timestamp, turbine_id, abnormal_sensor

# COMMAND ----------

# Example usage (uncomment to run):
# print_generation_summary()

# If running as main script:
if __name__ == "__main__":
    print_generation_summary()

