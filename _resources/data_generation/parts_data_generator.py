import pandas as pd
from faker import Faker
import random as rd

from .sensor_data_generator import get_sensor_impact_categories


# Read in the stock location data from local csv
# stock_data = pd.read_csv('./platform_csvs/PdM_Platform_Data - navy_stock_location_data.csv')

#For each supply location, we'll generate supply chain parts
# Low failure items + high failure items

PARTS = [
  {'name': 'Vane - Turbine'}, 
  {'name': 'Blade - Turbine'}, 
  {'name': 'Fuel Nozzle'}, 
  {'name': 'Seal'}, 
  {'name': 'controller card #1 - ECU'}, 
  {'name': 'controller card #2 - ECU'}, 
  {'name': 'Pump - Fuel'}, 
  {'name': 'Filter - Fuel / Oil'}, 
  {'name': 'Valve - Fuel / Oil'}
  ]

def import_parts_data(path):
  return pd.read_csv(path)

def generate_parts_data(spark):
  
  Faker.seed(0)
  faker = Faker()
  
  # list to check against high impact vs low impact
  high_impact_parts = ['Vane - Turbine', 'Blade - Turbine']
  # sensors = get_sensor_columns(folder_sensor, spark)

  # Get sensor categorization from the sensor data generator
  sensor_categories = get_sensor_impact_categories()

  parts_meta = []
  for p in PARTS:
    # Associate parts with appropriate sensors based on maintenance impact level
    nsn = faker.ean(length=8)
    part = {}
    part['NSN'] = nsn
    part['type'] = p['name']
    part['width'] = rd.randint(100,2000)
    part['height'] = rd.randint(100,2000)
    part['weight'] = rd.randint(1,1000)
    part['cost'] = rd.uniform(100.0, 10000.0)
    part['production_time'] = rd.randint(30,360)
    if p['name'] in high_impact_parts:
      part['sensors'] = sensor_categories['high_impact_sensors']
    else:
      # Parts can be associated with moderate or low impact sensors
      part['sensors'] = rd.sample(sensor_categories['all_non_high_impact'], rd.randint(1,2))
    parts_meta.append(part)

  return spark.createDataFrame(pd.DataFrame(parts_meta))


def generate_inventory_for_locations(spark, stock_data, PARTS):
    inventory_rows = []
    for _, row in stock_data.iterrows():
        for part in PARTS:
            inventory_rows.append({
                'stock_location': row['stock_location'],
                'stock_location_id': row['stock_location_id'],
                'lat': row['lat'],
                'long': row['long'],
                'part_name': part['name'],
                'inventory_level': rd.randint(0, 10),
                'provisioning_time': rd.randint(0, 5)
            })
    return spark.createDataFrame(pd.DataFrame(inventory_rows))

# Example usage:
# stock_data = pd.read_csv('your_stock_data.csv')
# inventory_df = generate_inventory_for_locations(stock_data, PARTS)
# display(spark.createDataFrame(inventory_df))


def get_parts_and_stock(spark, path):
  parts_meta = generate_parts_data(spark)
  stock_data = import_parts_data(path)
  inventory = generate_inventory_for_locations(spark, stock_data, PARTS)

  inventory_joined = parts_meta.join(inventory, parts_meta.type == inventory.part_name, "inner").drop('part_name')

  return parts_meta, inventory_joined

