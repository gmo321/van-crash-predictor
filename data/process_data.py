import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col, count, first
from pyspark.ml.feature import Imputer
from pyspark.sql.types import IntegerType, BooleanType, DateType
from pyspark.sql.functions import when, col, avg




def main(spark):
    
    no_cf_df = spark.read.parquet('data/parquet/TAS/no_cf')
    no_city_df = spark.read.parquet('data/parquet/TAS/no_city')
    entity_df = spark.read.parquet('data/parquet/TAS/entity')
    icbc_df = spark.read.parquet('data/parquet/icbc')
    
    # Read parquet files
    #no_cf_df = spark.read.parquet('s3a://van-crash-data/TAS/processed-data/no_cf')
    #no_city_df = spark.read.parquet('s3a://van-crash-data/TAS/processed-data/no_city')
    #entity_df = spark.read.parquet('s3a://van-crash-data/TAS/processed-data/entity')
    #icbc_df = spark.read.parquet('s3a://van-crash-data/icbc/processed-data/')

    
    ### CLEAN TAS DATA SETS ###
    
    # Remove municipalities with value "UNKNOWN"
    no_cf_df = no_cf_df.filter(~col("municipality").contains("UNKNOWN"))
    
    # Find count of null "speed_limit_km_h" for each municipality
    null_counts_df = no_cf_df.groupBy("municipality").agg(F.sum(F.when(F.col("speed_limit_km_h").isNull(), 1).otherwise(0)).alias("null_speed_count"),
                                         F.count('speed_limit_km_h').alias('total_count'))
    
    all_nulls_df = null_counts_df.filter(F.col('null_speed_count') == F.col('total_count'))
    
    # Filter out municipalities where all speed_limit_km_h are null
    no_cf_df = no_cf_df.join(all_nulls_df, on='municipality', how='left_anti')
    
    # Data imputation of "speed_limit_km_h" with mode of each municipality
    # Repartition the DataFrame by municipality
    no_cf_df = no_cf_df.repartition("municipality")
    
    mode_speed_df = no_cf_df.groupBy('municipality', 'speed_limit_km_h').agg(count('*').alias('count')) \
        .orderBy('municipality', F.col('count').desc())
        
    # Use a window function to pick the most frequent speed
    window_spec = Window.partitionBy("municipality").orderBy(F.col("count").desc())
    mode_speed_df = mode_speed_df.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .drop("count", "rank")
        
    no_cf_df = no_cf_df.join(F.broadcast(mode_speed_df.withColumnRenamed('speed_limit_km_h', 'mode_speed_limit')),
                            on='municipality',
                            how='left').withColumn('speed_limit_km_h', F.when(F.col('speed_limit_km_h').isNull(), F.col('mode_speed_limit')) \
                            .otherwise(F.col('speed_limit_km_h'))).drop('mode_speed_limit')
                            
    
    # Data Imputation of remaining null "speed_limit_km_h" with mode 
    imputer = Imputer(
        inputCols=['speed_limit_km_h'],
        outputCols=['speed_limit_km_h'],
        strategy='mode'
    )
    
    model = imputer.fit(no_cf_df)
    
    no_cf_df = model.transform(no_cf_df)
    

    
    ### CLEAN ICBC DATASET ###
    
    # TODO: Geocode lon, lat values
    # Comment this code to see null lon, lat values
    icbc_df = icbc_df.dropna()
    
    # Rename columns for consistency
    icbc_df = icbc_df.withColumnsRenamed({'pedestrian_flag':'pedestrian_involved', 
                                          'total_victims': 'total_casualty'})
    
    # Cast column types
    icbc_df = icbc_df.withColumn('is_intersection_crash', col('is_intersection_crash').cast(BooleanType())) \
        .withColumn('pedestrian_involved', col('pedestrian_involved').cast(BooleanType()))
        
    
    # Remove municipalities with value "UNKNOWN"
    icbc_df = icbc_df.filter(~col("municipality").contains("UNKNOWN"))
    
    # Convert string columns to lowercase
    icbc_df = icbc_df.withColumn('crash_severity', lower('crash_severity')) \
                    .withColumn('day', lower('day')) \
                    .withColumn('crash_configuration', lower('crash_configuration')) \
                    .withColumn('month', lower('month')) \
                    .withColumn('region', lower('region')) \
                    .withColumn('street', lower('street')) \
                    .withColumn('municipality', lower('municipality')) \
                    .withColumn('cross_street', lower('cross_street')) \
                    .withColumn('city', lower('city'))
        
    #icbc_df.groupBy('municipality').agg(sum(when(col('longitude').isNull(), 1))).alias('null_longitude_count').show()
        
    #icbc_df.groupBy('municipality').agg(count('*').alias('count')).orderBy(col('count').desc()).show(truncate=False)
    
    #print(icbc_df.select('municipality').distinct().count()) #835 unique municipalities
    
    #icbc_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in icbc_df.columns]).show()
    # Lon, Lat 264503 nulls
    # cross_street 736558 nulls

    
    
    ### MERGE DATASETS ###
    
    # Merge dataframes
    merged_df1 = no_cf_df.join(no_city_df, on=["region", 
                                              "year", 
                                              "month", 
                                              "light",
                                              "accident_type", 
                                              "collision_type", 
                                              "crash_configuration",
                                              "pedestrian_involved",
                                              "road_condition",
                                              "weather",
                                              "road_surface",
                                              "speed_limit_km_h",
                                              "speed_advisory",
                                              "traffic_control",
                                              "traffic_flow",
                                              "total_casualty",
                                              "total_vehicles_involved"],
                              how='inner')
    
    
    merged_df2 = merged_df1.join(entity_df, on=["region", 
                                              "year", 
                                              "month", 
                                              "accident_type", 
                                              "collision_type", 
                                              "crash_configuration"],
                              how='inner')
    
    # Cast proper types
    merged_df2 = merged_df2.withColumn('year', col('year').cast(IntegerType())) \
        .withColumn('pedestrian_involved', col('pedestrian_involved').cast(BooleanType())) \
        .withColumn('total_casualty', col('total_casualty').cast(IntegerType())) \
        .withColumn('total_vehicles_involved', col('total_vehicles_involved').cast(IntegerType()))
    
    # Convert string to lowercase 
    merged_df2 = merged_df2.withColumn('region', lower('region')) \
                    .withColumn('month', lower('month')) \
                    .withColumn('accident_type', lower('accident_type')) \
                    .withColumn('collision_type', lower('collision_type')) \
                    .withColumn('crash_configuration', lower('crash_configuration')) \
                    .withColumn('light', lower('light')) \
                    .withColumn('road_condition', lower('road_condition')) \
                    .withColumn('weather', lower('weather')) \
                    .withColumn('road_surface', lower('road_surface')) \
                    .withColumn('traffic_control', lower('traffic_control')) \
                    .withColumn('traffic_flow', lower('traffic_flow')) \
                    .withColumn('municipality', lower('municipality')) \
                    .withColumn('damage_location', lower('damage_location')) \
                    .withColumn('damage_severity', lower('damage_severity')) \
                    .withColumn('speed_involved', lower('speed_involved')) 
                                
    
    merged_df2 = merged_df2.withColumnRenamed('accident_type', 'crash_severity')
    
    
    # Merge icbc_df with merged_df2 (TAS datasets)
    merged_all_df = icbc_df.join(merged_df2, on=["municipality",
                                                  "year",
                                                  "month",
                                                  "region",
                                                  "crash_configuration",
                                                  "total_casualty",
                                                  "pedestrian_involved",
                                                  "crash_severity"],
                                  how='inner')
    
    
    # Drop unneeded columns
    merged_all_df = merged_all_df.drop('crash_severity', 
                                       'crash_configuration', 
                                       'collision_type', 
                                       'damage_location', 
                                       'traffic_flow', 
                                       'traffic_control', 
                                       'speed_advisory', 
                                       'exceeding_speed', 
                                       'excessive_speed',
                                       'driver_inattentive',
                                       'alcohol_involved',
                                       'driving_too_fast',
                                       'fell_asleep',
                                       'pre_action',
                                       'light',
                                       )
    
    

    ### FEATURE ENGINEERING ###
    # TODO
    # Geospatial data

    # Create columnn "day_numeric" to convert "day" column to numerical format (e.g Monday = 0, Tuesday = 2, etc.)
    merged_all_df = merged_all_df.withColumn('day_numeric',
                                             F.when(col('day') == 'monday', 0)
                                             .when(col('day') == 'tuesday', 1)
                                             .when(col('day') == 'wednesday', 2)
                                             .when(col('day') == 'thursday', 3)
                                             .when(col('day') == 'friday', 4)
                                             .when(col('day') == 'saturday', 5)
                                             .when(col('day') == 'sunday', 6)
                                             .cast(IntegerType()))
    
    
    # Create "is_weekend" flag column
    merged_all_df = merged_all_df.withColumn('is_weekend',
                                             F.when((col('day_numeric') == 5) | (col('day_numeric') == 6), 1).otherwise(0))
        
                                        
    # Create column "month_numeric" to convert "month" column to numeric format (e.g January = 1, February = 2, etc.)
    merged_all_df = merged_all_df.withColumn('month_numeric',
                                             F.when(col('month') == 'january', 1)
                                             .when(col('month') == 'february', 2)
                                             .when(col('month') == 'march', 3)
                                             .when(col('month') == 'april', 4)
                                             .when(col('month') == 'may', 5)
                                             .when(col('month') == 'june', 6)
                                             .when(col('month') == 'july', 7)
                                             .when(col('month') == 'august', 8)
                                             .when(col('month') == 'september', 9)
                                             .when(col('month') == 'october', 10)
                                             .when(col('month') == 'november', 11)
                                             .when(col('month') == 'december', 12)
                                             .cast(IntegerType()))
    
    
    # Create column "time_period" to group "time" periods to time buckets (e.g "morning", "afternoon", etc.)
    merged_all_df = merged_all_df.withColumn('time_period',
                                             F.when((col('time') == '06:00-08:59') | (col('time') == '09:00-11:59'), 'morning')
                                             .when((col('time') == '12:00-14:59') | (col('time') == '15:00-17:59'), 'afternoon')
                                             .when((col('time') == '18:00-20:59') | (col('time') == '21:00-23:59'), 'evening')
                                             .when((col('time') == '00:00-02:59') | (col('time') == '03:00-05:59'), 'night'))
    
    # Create column "is_rush_hour" to identify common rush hour periods (e.g '06:00-08:59' or '03:00-05:59')
    merged_all_df = merged_all_df.withColumn('is_rush_hour',
                                             F.when((col('time') == '06:00-08:59') | (col('time') == '15:00-17:59'), 1).otherwise(0))
    
    
    # Create column "season" to categorize months to season
    merged_all_df = merged_all_df.withColumn("season",
                                             F.when((col('month_numeric') == '3') | 
                                                    (col('month_numeric') == '4') | 
                                                    (col('month_numeric') == '5'), 'spring')
                                             .when((col('month_numeric') == '6') | 
                                                   (col('month_numeric') == '7') | 
                                                   (col('month_numeric') == '8'), 'summer')
                                             .when((col('month_numeric') == '9') | 
                                                   (col('month_numeric') == '10') | 
                                                   (col('month_numeric') == '11'), 'autumn')
                                             .when((col('month_numeric') == '12') | 
                                                   (col('month_numeric') == '1') | 
                                                   (col('month_numeric') == '2'), 'winter'))
    
    
    # Create general driver distracted column
    #merged_all_df = merged_all_df.withColumn("risk_factor",
    #                                         F.when((col("speed_involved") == 1) | (col("distraction_involved") == 1) | 
    #                                                (col("impaired_involved") == 1) | (col("drug_involved") == 1), 1).otherwise(0))
    
    # Create column "crash_severity"
    # crash_severity: minor, moderate, severe
    merged_all_df = merged_all_df.withColumn("crash_severity",
                                             when(
                                                 (col("total_casualty") >= 3) |
                                                 (col("total_crashes") >= 3) |
                                                 (col("damage_severity").isin(["demolished(repair impractical)", "severe"])) | 
                                                 (col("total_vehicles_involved") >= 3), "severe"
                                             ).when(
                                                 (col("total_casualty") == 0) | (col("total_casualty") == 1) |
                                                 (col("total_crashes") == 1) &
                                                 (col("damage_severity").isin(["minor", "no damage (none visible)"])), "minor"
                                             ).when(
                                                 col("total_casualty").between(1, 2) |
                                                 col("total_crashes").between(1, 2) &
                                                 (col("damage_severity") == "moderate"), "moderate"
                                             ).otherwise("unknown"))
    

    
    merged_all_df = merged_all_df.select('latitude', 'longitude', 'year', 'month', 'day', 'municipality', 'region', 'total_casualty', 'is_intersection_crash', 'street', 
                                         'total_crashes', 'cross_street', 'weather', 'road_condition', 'road_surface', 'speed_limit_km_h', 'pedestrian_involved', 
                                         'total_vehicles_involved', 'distraction_involved', 'drug_involved', 'impaired_involved', 'speed_involved', 
                                         'is_weekend', 'time_period', 'is_rush_hour', 'season', 'crash_severity')
 
    # Converting to binary values
    merged_all_df = merged_all_df.withColumn('pedestrian_involved', F.when(F.col('pedestrian_involved') == 'true', 1).otherwise(0)) \
            .withColumn('distraction_involved', F.when(F.col('distraction_involved') == 'Yes', 1).otherwise(0)) \
            .withColumn('drug_involved', F.when(F.col('drug_involved') == 'Yes', 1).otherwise(0)) \
            .withColumn('impaired_involved', F.when(F.col('impaired_involved') == 'Yes', 1).otherwise(0)) \
            .withColumn('speed_involved', F.when(F.col('speed_involved') == 'yes', 1).otherwise(0)) \
            .withColumn('is_weekend', F.when(F.col('is_weekend') == 1, 1).otherwise(0)) \
            .withColumn('is_rush_hour', F.when(F.col('is_rush_hour') == 1, 1).otherwise(0)) \
            .withColumn('is_intersection_crash', F.when(F.col('is_intersection_crash') == 'true', 1).otherwise(0))
                
       
    #merged_all_df = merged_all_df.select('is_intersection_crash', 'distraction_involved', 'drug_involved', 'impaired_involved', 'speed_involved', 'is_weekend', 'is_rush_hour').show()
    #merged_all_df.show(20, truncate=False)

    # Unknown value in columns: weather, road_condition, road_surface
    # Drop duplicate rows
    merged_all_df = merged_all_df.dropDuplicates()

    merged_all_df.write.parquet("data/parquet/merged", compression='LZ4', mode='overwrite')
    

if __name__ == '__main__':  
     
    spark = SparkSession.builder \
        .appName('Data Processing') \
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