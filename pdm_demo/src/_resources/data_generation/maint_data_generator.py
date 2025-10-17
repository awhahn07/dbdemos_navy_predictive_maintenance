from pyspark.sql.types import StructType, StructField, StringType, BooleanType, FloatType, ArrayType


MAINT_DATA = [
    ('sensor_A', 'Intermediate Level', True, 8.0),    # Compressor related - moderate impact
    ('sensor_B', 'Intermediate Level', True, 10.5),   # Existing - moderate impact  
    ('sensor_C', 'Organizational Level', True, 6.0),    # Cooling system - lower impact
    ('sensor_D', 'Organizational Level', True, 5.0),    # Existing - lower impact
    ('sensor_E', 'Intermediate Level', True, 12.0),   # Fuel system - moderate impact
    ('sensor_F', 'Depot Level', False, 24.0) # Existing - high impact (turbine blades/vanes)
]


def generate_maint_data(spark):
    sensor_schema = StructType(
        [
            StructField("fault", StringType(), True),
            StructField("maintenance_type", StringType(), True),
            StructField("operable", BooleanType(), True),
            StructField("ttr", FloatType(), True),
            ]
        )
    
    return spark.createDataFrame(MAINT_DATA, sensor_schema)
