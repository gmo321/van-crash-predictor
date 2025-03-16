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
    
    #TODO: Geocode lon, lat values
    icbc_df = icbc_df.dropna()
    
    # Remove municipalities with value "UNKNOWN"
    icbc_df = icbc_df.filter(~col("municipality").contains("UNKNOWN"))
    
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
                                              "on_road",
                                              "pedestrian_involved",
                                              "road_condition",
                                              "weather",
                                              "road_surface",
                                              "speed_limit_km_h",
                                              "speed_advisory",
                                              "traffic_control",
                                              "traffic_flow",
                                              "total_casualty",
                                              "total_vehicles_involved"
                                              ],
                              how='inner')
    
    # Rename columns for consistency
    icbc_df = icbc_df.withColumnsRenamed({'pedestrian_flag':'pedestrian_involved', 
                                          'total_victims': 'total_casualty'})

    merged_df2 = merged_df1.join(entity_df, on=["region", 
                                              "year", 
                                              "month", 
                                              "accident_type", 
                                              "collision_type", 
                                              "crash_configuration"],
                              how='inner')
    
    
    merged_df2 = merged_df2.withColumn('year', col('year').cast(IntegerType())) \
        .withColumn('total_casualty', col('total_casualty').cast(IntegerType()))
        
    
    icbc_df = icbc_df.withColumn('month', lower('month'))
    merged_df2 = merged_df2.withColumn('month', lower('month'))

    
    # Rename columns in merged_df2 to avoid duplicated columns after join
    merged_df2 = merged_df2.withColumnRenamed('region', 'region_df2') \
        .withColumnRenamed('crash_configuration', 'crash_configuration_df2') \
        .withColumnRenamed('pedestrian_involved', 'pedestrian_involved_df2') \
        .withColumnRenamed('total_casualty', 'total_casualty_df2')
    
    # Merge icbc_df with merged_df2 (TAS datasets)
    merged_all_df = icbc_df.join(merged_df2, on=["municipality",
                                                  "year",
                                                  "month"],
                                  how='inner')
    
    # Drop duplicate columns
    merged_all_df = merged_all_df.drop('region_df2', 'crash_configuration_df2', 'pedestrian_involved_df2', 'total_casualty_df2')

    #print(merged_all_df.columns)
    
    #print(merged_all_df.count()) #1086563132
    
    #merged_all_df.show(5, truncate=False)
    
    #merged_all_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in merged_all_df.columns]).show()
    
    ### FEATURE ENGINEERING ###
    # TODO
    # 1. Make time categories for time e.g morning, afternoon, evening, night
    # 3. Categorize to seasons
    # 4. Include peak traffic indicators
    
    #merged_all_df = merged_all_df.select(F.make_date(col('year'), col('month')).alias('date')).show()

    
    # Create columnn "day_numeric" to convert "day" column to numerical format (e.g Monday = 0, Tuesday = 2, etc.)
    merged_all_df = merged_all_df.withColumn('day_numeric',
                                             F.when(col('day') == 'MONDAY', 0)
                                             .when(col('day') == 'TUESDAY', 1)
                                             .when(col('day') == 'WEDNESDAY', 2)
                                             .when(col('day') == 'THURSDAY', 3)
                                             .when(col('day') == 'FRIDAY', 4)
                                             .when(col('day') == 'SATURDAY', 5)
                                             .when(col('day') == 'SUNDAY', 6)
                                             .cast(IntegerType()))
    
    
    # Create "is_weekend" flag column
    merged_all_df = merged_all_df.withColumn('is_weekend',
                                             F.when((col('day_numeric') == 5) | (col('day_numeric') == 6), 1).otherwise(0))
                                        
    # Create column "month_numeric" to convert "month" column to numeric format (e.g January = 1, February = 2, etc.)
    merged_all_df = merged_all_df.withColumn('month_numeric',
                                             F.when(col('month') == 'janurary', 1)
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
    
    #merged_all_df.select('time', 'time_period', 'day', 'day_numeric', 'is_weekend', 'month', 'month_numeric', 'season').show()
    
    #merged_all_df.select('crash_severity', 'crash_configuration', 'total_crashes', 'total_casualty', 'accident_type', 'collision_type', 'damage_severity').show(truncate=False)
    
    # TODO
    # Create rush hour flag?
    # Geospatial info: 
    
    #print(merged_all_df.columns)
    #['municipality', 'year', 'month', 'crash_severity', 'day', 'crash_configuration', 'is_intersection_crash', 'pedestrian_involved', 'region', 'street', 'time', 
    # 'total_crashes', 'total_casualty', 'latitude', 'cross_street', 'longitude', 'city', 'accident_type', 'collision_type', 'light', 'on_road', 'road_condition', 
    # 'weather', 'road_surface', 'speed_limit_km_h', 'speed_advisory', 'traffic_control', 'traffic_flow', 'total_vehicles_involved', 'alcohol_involved', 
    # 'distraction_involved', 'driving_too_fast', 'drug_involved', 'exceeding_speed', 'excessive_speed', 'fell_asleep', 'impaired_involved', 'speed_involved', 
    # 'driver_in_ext_distraction', 'driver_inattentive', 'driving_without_due_care', 'age_range', 'gender', 'vehicle_type', 'damage_location', 'damage_severity', 
    # 'pre_action', 'vehicle_body_style', 'vehicle_make', 'vehicle_model_year']
    #merged_all_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in merged_all_df.columns]).show() 
    #merged_all_df.show(5, truncate=False)
    
    

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