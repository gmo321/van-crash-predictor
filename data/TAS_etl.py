import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col, count, first
from pyspark.ml.feature import Imputer




def main(spark):
    
    no_cf_df_schema = types.StructType([
        StructField("Municipality", StringType(), True),
        StructField("Year", IntegerType(), True),
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
        StructField("Crash Count", IntegerType(), True),
        StructField("Total Casualty", IntegerType(), True),
        StructField("Total Vehicles Involved", IntegerType(), True),
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
        StructField("Year", IntegerType(), True),
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
        StructField("Crash Count", IntegerType(), True),
        StructField("Total Casualty", IntegerType(), True),
        StructField("Total Vehicles Involved", IntegerType(), True),
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
        StructField("Year", IntegerType(), True),
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
        StructField("Entity Count", IntegerType(), True),
        StructField("Collision Type", StringType(), True),
        StructField("Damage Location", StringType(), True),
        StructField("Damage Severity", StringType(), True),
        StructField("Driver License Jurisdiction", StringType(), True),
        StructField("Pre Action", StringType(), True),
        StructField("Travel Direction", StringType(), True),
        StructField("Vehicle Body Style", StringType(), True),
        StructField("Vehicle Jurisdiction", StringType(), True),
        StructField("Vehicle Make", StringType(), True),
        StructField("Vehicle Model Year", IntegerType(), True)
])
    
    
    no_cf_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("ignoreLeadingWhiteSpace", "true") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_no_CF.csv", schema=no_cf_df_schema).repartition(60)
                        
    no_city_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_no_city.csv", schema=no_city_schema).repartition(60)
                        
                        
    entity_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/TAS/raw/TAS_entity.csv", schema=entity_schema).repartition(60)
                        
    #total_rows_1 = no_cf_df.count() 107020 
    #total_rows_2 = no_city_df.count() 106841
    #total_rows_3 = entity_df.count() 196395

    # Drop unneccessary columns
    no_cf_df = no_cf_df.drop('In Parking Lot', 
                             'Cyclist Involved', 
                             'Hit And Run Indicator', 
                             'Crash Count', 
                             'Motorcycle Involved',
                             'Land Use',
                             'Impact With Animal',
                             'Pedestrian Activity',
                             'Road Character',
                             'On Road',
                             'Road Class')
    
    no_city_df = no_city_df.drop('Impact With Animal',
                                 'In Parking Lot', 
                                 'Hit And Run Indicator',
                                 'Impact With Animal',
                                 'Cyclist Involved',
                                 'Motorcycle Involved',
                                 'Land Use', 
                                 'Pedestrian Activity', 
                                 'Road Character', 
                                 'Road Class', 
                                 'Crash Count',
                                 'On Road',
                                 'Driver In Ext Distraction',
                                 'Driving Without Due Care',
                                 'Communication Video Equipment')
    
    entity_df = entity_df.drop('Driver License Jurisdiction', 
                               'Vehicle Jurisdiction', 
                               'Age Range',
                               'Gender',
                               'Vehicle Make',
                               'Vehicle Model Year',
                               "Entity Type", 
                               "Entity Count", 
                               "Travel Direction",
                               'Contributing Factor 1', 
                               'Contributing Factor 2', 
                               'Contributing Factor 3', 
                               'Contributing Factor 4',
                               'Vehicle Type',
                               'Vehicle Body Style',
                               'Vehicle Use')
    
    
    # Check for nulls
    #no_cf_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_cf_df.columns]).show() # Speed zone - 30
    #no_city_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_city_df.columns]).show()
    #entity_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in entity_df.columns]).show() # Collision Type - 88
    
    #no_city_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in no_city_df.columns]).show()
    
    # Drop null rows
    no_cf_df = no_cf_df.dropna()
    no_city_df = no_city_df.dropna()
    entity_df = entity_df.dropna()
    
    # Renaming columns for consistency    
    no_cf_df = no_cf_df.withColumnsRenamed({'Municipality': 'municipality', 
                                          'Year': 'year', 
                                          'Accident Type': 'accident_type', 
                                          'Collision Type': 'collision_type',
                                          'Crash Configuration': 'crash_configuration', 
                                          'Cyclist Involved': 'cyclist_involved',
                                          'Hit And Run Indicator':'hit_and_run_indicator', 
                                          'Impact With Animal': 'impact_with_animal',
                                          'Month': 'month',
                                          'Motorcycle Involved': 'motorcycle_involved',
                                          'Pedestrian Involved':'pedestrian_involved',
                                          'Region':'region',
                                          'Road Condition':'road_condition',
                                          'Weather':'weather',
                                          'Total Casualty': 'total_casualty',
                                          'Total Vehicles Involved': 'total_vehicles_involved',
                                          'Road Surface':'road_surface',
                                          'Speed Zone':'speed_zone',
                                          'Traffic Control':'traffic_control',
                                          'Traffic Flow':'traffic_flow',
                                          'Light':'light',
                                          'On Road':'on_road',
                                          'Speed Advisory':'speed_advisory'})
    
    no_city_df = no_city_df.withColumnsRenamed({'Region': 'region', 
                                                'Year': 'year', 
                                                'Accident Type': 'accident_type', 
                                                'Alcohol Involved': 'alcohol_involved', 
                                                'Collision Type': 'collision_type', 
                                                'Crash Configuration': 'crash_configuration', 
                                                'Cyclist Involved': 'cyclist_involved', 
                                                'Distraction Involved': 'distraction_involved', 
                                                'Driving Too Fast': 'driving_too_fast', 
                                                'Drug Involved': 'drug_involved', 
                                                'Exceeding Speed': 'exceeding_speed', 
                                                'Excessive Speed': 'excessive_speed', 
                                                'Fell Asleep': 'fell_asleep', 
                                                'Impact With Animal': 'impact_with_animal', 
                                                'Impaired Involved': 'impaired_involved', 
                                                'Month': 'month', 
                                                'Motorcycle Involved': 'motorcycle_involved', 
                                                'Pedestrian Involved': 'pedestrian_involved', 
                                                'Road Condition': 'road_condition', 
                                                'Speed Involved': 'speed_involved', 
                                                'Weather': 'weather', 
                                                'Crash Count': 'crash_count', 
                                                'Total Casualty': 'total_casualty', 
                                                'Total Vehicles Involved': 'total_vehicles_involved', 
                                                'Driver In Ext Distraction': 'driver_in_ext_distraction', 
                                                'Driver Inattentive': 'driver_inattentive', 
                                                'Driving Without Due Care': 'driving_without_due_care', 
                                                'Hit And Run Indicator': 'hit_and_run_indicator', 
                                                'Road Surface': 'road_surface', 
                                                'Speed Zone': 'speed_zone', 
                                                'Traffic Control': 'traffic_control', 
                                                'Traffic Flow': 'traffic_flow',
                                                'Light':'light',
                                                'On Road':'on_road',
                                                'Speed Advisory':'speed_advisory'})
    
    entity_df = entity_df.withColumnsRenamed({'Region': 'region',
                                            'Year': 'year',
                                            'Accident Type': 'accident_type',
                                            'Contributing Factor 1': 'contributing_factor_1',
                                            'Contributing Factor 2': 'contributing_factor_2',
                                            'Contributing Factor 3': 'contributing_factor_3',
                                            'Contributing Factor 4': 'contributing_factor_4',
                                            'Crash Configuration': 'crash_configuration',
                                            'Month': 'month',
                                            'Vehicle Type': 'vehicle_type',
                                            'Vehicle Use': 'vehicle_use',
                                            'Collision Type': 'collision_type',
                                            'Damage Location': 'damage_location',
                                            'Damage Severity': 'damage_severity',
                                            'Pre Action': 'pre_action',
                                            'Vehicle Body Style': 'vehicle_body_style'})
    
    # Regex to remove unwanted (non-ASCII) characters
    no_cf_df = no_cf_df.withColumn("traffic_flow", F.regexp_replace(F.col("traffic_flow"), "[^\x00-\x7F]", ""))
    no_city_df = no_city_df.withColumn("traffic_flow", F.regexp_replace(F.col("traffic_flow"), "[^\x00-\x7F]", ""))
    
    # Change column 'speed_zone' to 'speed_limit' and extract speed in Km/H
    no_cf_df = no_cf_df.withColumn('speed_limit_km_h', regexp_extract('speed_zone', r'(\d+)', 0)).drop('speed_zone')
    no_city_df = no_city_df.withColumn('speed_limit_km_h', regexp_extract('speed_zone', r'(\d+)', 0)).drop('speed_zone')
    
    no_cf_df = no_cf_df.withColumn('speed_limit_km_h', F.col('speed_limit_km_h').cast('integer'))
    no_city_df = no_city_df.withColumn('speed_limit_km_h', F.col('speed_limit_km_h').cast('integer'))
    
    
    # Check the distribution of data by municipality
    #no_cf_df.groupBy("municipality").count().orderBy("count", ascending=False).show()

    # Write parquet files
    no_cf_df.write.parquet("data/parquet/TAS/no_cf", compression='LZ4', mode='overwrite')
    no_city_df.write.parquet("data/parquet/TAS/no_city", compression='LZ4', mode='overwrite')
    entity_df.write.parquet("data/parquet/TAS/entity", compression='LZ4', mode='overwrite')
    
    #try:
    #    no_cf_df.write.parquet("s3a://van-crash-data/TAS/processed-data/no_cf", compression='LZ4', mode='overwrite')
    #    no_city_df.write.parquet("s3a://van-crash-data/TAS/processed-data/no_city", compression='LZ4', mode='overwrite')
    #    entity_df.write.parquet("s3a://van-crash-data/TAS/processed-data/entity", compression='LZ4', mode='overwrite')
    #except Exception as e:
    #    print(f"Error writing to S3: {e}")



if __name__ == '__main__':  
    spark = SparkSession.builder \
        .appName('Traffic Accident System etl') \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.yarn.executor.memoryOverhead", "1g") \
        .config("spark.sql.shuffle.partitions", "60") \
        .config("spark.executor.cores", "3") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.maxExecutors", "4") \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)