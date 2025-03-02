import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql import functions as F


def main(spark):
    no_cf_path = 'raw/TAS_no_CF.csv'
    no_city_path = 'raw/TAS_no_city.csv'
    entity_path = 'raw/TAS_entity.csv'
    
    no_cf_df_schema = types.StructType([
    StructField("Municipality", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Accident Type", StringType(), True),
    StructField("Collision Type", StringType(), True),
    StructField("Crash Configuration", StringType(), True),
    StructField("Cyclist Involved", StringType(), True),
    StructField("Hit And Run Indicator", StringType(), True),
    StructField("Impact With Animal", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Motorcycle Involved", StringType(), True),
    StructField("Pedestrian Involved", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Road Condition", StringType(), True),
    StructField("Weather", StringType(), True),
    StructField("Crash Count", StringType(), True),
    StructField("Total Casualty", StringType(), True),
    StructField("Total Vehicles Involved", StringType(), True),
    StructField("In Parking Lot", StringType(), True),
    StructField("Land Use", StringType(), True),
    StructField("Light", StringType(), True),
    StructField("On Road", StringType(), True),
    StructField("Pedestrian Activity", StringType(), True),
    StructField("Road Character", StringType(), True),
    StructField("Road Class", StringType(), True),
    StructField("Road Surface", StringType(), True),
    StructField("Speed Advisory", StringType(), True),
    StructField("Speed Zone", StringType(), True),
    StructField("Traffic Control", StringType(), True),
    StructField("Traffic Flow", StringType(), True)
])
  
    no_city_schema = types.StructType([
    StructField("Region", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Accident Type", StringType(), True),
    StructField("Alcohol Involved", StringType(), True),
    StructField("Collision Type", StringType(), True),
    StructField("Crash Configuration", StringType(), True),
    StructField("Cyclist Involved", StringType(), True),
    StructField("Distraction Involved", StringType(), True),
    StructField("Driving Too Fast", StringType(), True),
    StructField("Drug Involved", StringType(), True),
    StructField("Exceeding Speed", StringType(), True),
    StructField("Excessive Speed", StringType(), True),
    StructField("Fell Asleep", StringType(), True),
    StructField("Impact With Animal", StringType(), True),
    StructField("Impaired Involved", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Motorcycle Involved", StringType(), True),
    StructField("Pedestrian Involved", StringType(), True),
    StructField("Road Condition", StringType(), True),
    StructField("Speed Involved", StringType(), True),
    StructField("Weather", StringType(), True),
    StructField("Crash Count", StringType(), True),
    StructField("Total Casualty", StringType(), True),
    StructField("Total Vehicles Involved", StringType(), True),
    StructField("Communication Video Equipment", StringType(), True),
    StructField("Driver In Ext Distraction", StringType(), True),
    StructField("Driver Inattentive", StringType(), True),
    StructField("Driving Without Due Care", StringType(), True),
    StructField("Hit And Run Indicator", StringType(), True),
    StructField("In Parking Lot", StringType(), True),
    StructField("Land Use", StringType(), True),
    StructField("Light", StringType(), True),
    StructField("On Road", StringType(), True),
    StructField("Pedestrian Activity", StringType(), True),
    StructField("Road Character", StringType(), True),
    StructField("Road Class", StringType(), True),
    StructField("Road Surface", StringType(), True),
    StructField("Speed Advisory", StringType(), True),
    StructField("Speed Zone", StringType(), True),
    StructField("Traffic Control", StringType(), True),
    StructField("Traffic Flow", StringType(), True)
])
    
    entity_schema = types.StructType([
    StructField("Region", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Accident Type", StringType(), True),
    StructField("Age Range", StringType(), True),
    StructField("Contributing Factor 1", StringType(), True),
    StructField("Contributing Factor 2", StringType(), True),
    StructField("Contributing Factor 3", StringType(), True),
    StructField("Contributing Factor 4", StringType(), True),
    StructField("Crash Configuration", StringType(), True),
    StructField("Entity Type", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Vehicle Type", StringType(), True),
    StructField("Vehicle Use", StringType(), True),
    StructField("Entity Count", StringType(), True),
    StructField("Collision Type", StringType(), True),
    StructField("Damage Location", StringType(), True),
    StructField("Damage Severity", StringType(), True),
    StructField("Driver License Jurisdiction", StringType(), True),
    StructField("Pre Action", StringType(), True),
    StructField("Travel Direction", StringType(), True),
    StructField("Vehicle Body Style", StringType(), True),
    StructField("Vehicle Jurisdiction", StringType(), True),
    StructField("Vehicle Make", StringType(), True),
    StructField("Vehicle Model Year", StringType(), True)
])
    
    
    no_cf_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("ignoreLeadingWhiteSpace", "true") \
                        .option("encoding", "UTF-16") \
                        .csv(no_cf_path, schema=no_cf_df_schema)
                        
    no_city_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv(no_city_path, schema=no_city_schema)
                        
                        
    entity_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv(entity_path, schema=entity_schema)
                        
    no_cf_df.show(1)    
    no_city_df.show(1)
    entity_df.show(1)
                                          
   
    


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Traffic Accident System etl') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)