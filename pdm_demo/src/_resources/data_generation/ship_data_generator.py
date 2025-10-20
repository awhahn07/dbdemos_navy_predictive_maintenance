from pyspark.sql.functions import monotonically_increasing_id, abs, hash, explode, array, lit, concat
from pathlib import Path
import uuid

import pandas as pd

# BASE_CSV_PATH = Path("_resources/data_generation/platform_csvs")
# PUBSEC_PLATFORM_DATA = BASE_CSV_PATH / "PdM_Platform_Data - pubsec_platform_data.csv"
# NAVY_PLATFORM_DATA = BASE_CSV_PATH / "PdM_Platform_Data - navy_platform_data.csv"
NUM_TURBINES = 4

def load_plaform_meta(spark, path=None):
  if not path:
    raise Exception('Path not defined')
  else:
    file_location = path
  
  try:
    pandas_df = pd.read_csv(file_location)
  except:
    raise Exception(f'File {file_location} not found')

  platform_meta = spark.createDataFrame(pandas_df)
  platform_meta = platform_meta.withColumn('designator_id', abs(hash('designator')))
  return platform_meta


def create_platform_with_turbine(platform_meta, num_turbines=NUM_TURBINES):
    def gen_uuids(n):
        return [str(uuid.uuid4()) for _ in range(n)]
    from pyspark.sql.types import ArrayType, StringType
    from pyspark.sql.functions import udf

    gen_uuids_udf = udf(lambda _: gen_uuids(num_turbines), ArrayType(StringType()))
    df = platform_meta.withColumn("turbine_id", gen_uuids_udf(lit(0)))
    df = df.withColumn("turbine_id", explode("turbine_id"))
    return df

def create_homeport_ids(platform_meta):
  home_locations = platform_meta.select('home_location').distinct()
  home_locations = home_locations.withColumn('home_location_id', concat(lit('home_'), monotonically_increasing_id()))
  return platform_meta.join(home_locations, on='home_location')

def get_platform_meta(spark, path=None):
  platform_meta = load_plaform_meta(spark, path)
  platform_meta = create_homeport_ids(platform_meta)
  #platform_meta = create_platform_with_turbine(platform_meta)
  return platform_meta

def map_platform_to_turbine(platform_meta, turbine_ids, platform_count):
  platform_meta = platform_meta.withColumn('join_key', monotonically_increasing_id())
  turbine_ids = turbine_ids.withColumn('join_key', monotonically_increasing_id() % platform_count)
  return platform_meta.join(turbine_ids, on='join_key').drop('join_key')


