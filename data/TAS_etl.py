import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql import functions as F



def main(spark):
    #no_cf_path = 'raw/TAS_no_CF.csv'
    #no_city_path = 'raw/TAS_no_city.csv'
    #entity_path = 'raw/TAS_entity.csv'
    
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
                        .csv("s3a://van-crash-data/TAS/raw/TAS_no_CF.csv", schema=no_cf_df_schema)
                        
    no_city_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_no_city.csv", schema=no_city_schema)
                        
                        
    entity_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_entity.csv", schema=entity_schema)
                        
                        
    #total_rows_1 = no_cf_df.count() 107020 
    #total_rows_2 = no_city_df.count() 106841
    #total_rows_3 = entity_df.count() 196395
    #print(f'Total rows: {total_rows_1} \n',
    #      f'Total rows: {total_rows_2} \n',
    #      f'Total rows: {total_rows_3} \n')

    #entity_df.select(["Travel Direction"]).show(10, truncate=False)
    no_cf_df = no_cf_df.drop('In Parking Lot', 'Crash Count', 'Land Use', 'Light', 'On Road', 'Pedestrian Activity', 'Road Character', 'Road Class')
    
    no_city_df = no_city_df.drop('In Parking Lot', 'Land Use', 'Light', 'On Road', 'Pedestrian Activity', 'Road Character', 'Road Class', 
                                 'Communication Video Equipment')
    
    entity_df = entity_df.drop('Driver License Jurisdiction', 'Vehicle Jurisdiction', "Entity Type", "Entity Count", 'Vehicle Body Style', "Travel Direction")
    
    # Check for nulls
    #no_cf_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_cf_df.columns]).show() # Speed zone - 30
    #no_city_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_city_df.columns]).show()
    #entity_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in entity_df.columns]).show() # Collision Type - 88
    
    # Drop null rows
    no_cf_df = no_cf_df.dropna()
    no_city_df = no_city_df.dropna()
    entity_df = entity_df.dropna()
    
    no_cf_df.show(3, truncate=False)
    no_city_df.show(3, truncate=False)
    entity_df.show(3, truncate=False)

    # TODO clean columns:                                      
    # two way traffic
    # vehicle model year

    # Count the duplicate rows
    #original_count = no_cf_df.count()
    #deduped_count = no_cf_df.dropDuplicates().count()
    #duplicate_rows = original_count - deduped_count
    #print(f"Number of duplicate rows: {duplicate_rows}")
    
    #duplicates_df = no_cf_df.groupBy(no_cf_df.columns).count().filter("count > 1")
    #duplicates_df.show(truncate=False)
    
    #no_cf_df.write.options(compression='LZ4', mode='overwrite').parquet("parquet/TAS/no_cf")
    #no_city_df.write.options(compression='LZ4', mode='overwrite').parquet("parquet/TAS/no_city")
    #entity_df.write.options(compression='LZ4', mode='overwrite').parquet("parquet/TAS/entity")
    
    # Read parquet files
    #no_cf_df_parquet = spark.read.parquet('data/parquet/TAS/no_city')
    
    #no_cf_df_parquet.show()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Traffic Accident System etl') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)