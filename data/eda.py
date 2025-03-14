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




def main(spark):
    # Read parquet files
    no_cf_df = spark.read.parquet('s3a://van-crash-data/TAS/processed-data/no_cf')
    no_city_df = spark.read.parquet('s3a://van-crash-data/TAS/processed-data/no_city')
    entity_df = spark.read.parquet('s3a://van-crash-data/TAS/processed-data/entity')
    
    #print(no_city_df.select('region').distinct().count())
    
    #total_rows_1 = no_cf_df_parquet.count()  #105353 
    #total_rows_2 = no_city_df_parquet.count()  #105182 
    #total_rows_3 = entity_parquet.count()  #152274 
    #print(f'Total rows: {total_rows_1} \n',
    #      f'Total rows: {total_rows_2} \n',
    #      f'Total rows: {total_rows_3} \n')
    
    # Remove municipalities with value "UNKNOWN"
    no_cf_df = no_cf_df.filter(~col("municipality").contains("UNKNOWN"))
    
    #no_cf_df.filter(F.col("speed_limit_km_h").isNull()).select("municipality").distinct().show()
    null_counts_df = no_cf_df.groupBy("municipality").agg(F.sum(F.when(F.col("speed_limit_km_h").isNull(), 1).otherwise(0)).alias("null_speed_count"),
                                         F.count('speed_limit_km_h').alias('total_count'))
    
    all_nulls_df = null_counts_df.filter(F.col('null_speed_count') == F.col('total_count'))
    
    # Filter out municipalities where all speed_limit_km_h are null
    no_cf_df = no_cf_df.join(all_nulls_df, on='municipality', how='left_anti')
        
    # Repartition the DataFrame by municipality
    no_cf_df = no_cf_df.repartition("municipality")
    
    mode_speed_df = no_cf_df.groupBy('municipality', 'speed_limit_km_h').agg(count('*').alias('count')) \
        .orderBy('municipality', F.col('count').desc())
        
    # Use a window function to pick the most frequent speed
    window_spec = Window.partitionBy("municipality").orderBy(F.col("count").desc())
    mode_speed_df = mode_speed_df.withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .drop("count", "rank")
        
    no_cf_df = no_cf_df.join(broadcast(mode_speed_df.withColumnRenamed('speed_limit_km_h', 'mode_speed_limit')),
                            on='municipality',
                            how='left').withColumn('speed_limit_km_h', F.when(F.col('speed_limit_km_h').isNull(), F.col('mode_speed_limit')) \
                            .otherwise(F.col('speed_limit_km_h')))
                            
    
    # Data Imputation of remaining null speed_limit_km_h with mode
    imputer = Imputer(
        inputCols=['speed_limit_km_h'],
        outputCols=['speed_limit_km_h'],
        strategy='mode'
    )
    
    model = imputer.fit(no_cf_df)
    
    no_cf_df = model.transform(no_cf_df)

    
    # Merge dataframes
    '''
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
    
    #print(merged_df1.columns)
    #print(merged_df1.count()) #131019
    
    #merged_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in merged_df.columns]).show()

    merged_df2 = merged_df1.join(entity_df, on=["region", 
                                              "year", 
                                              "month", 
                                              "accident_type", 
                                              "collision_type", 
                                              "crash_configuration"
                                              ],
                              how='inner')
    '''
    
    
    #print(merged_df2.columns)
    #['region', 'year', 'month', 'accident_type', 'collision_type', 'crash_configuration', 'light', 'on_road', 'pedestrian_involved', 'road_condition', 'weather', 
    # 'road_surface', 'speed_limit_km_h', 'speed_advisory', 'traffic_control', 'traffic_flow', 'total_casualty', 'total_vehicles_involved', 'municipality', 
    # 'alcohol_involved', 'distraction_involved', 'driving_too_fast', 'drug_involved', 'exceeding_speed', 'excessive_speed', 'fell_asleep', 'impaired_involved', 
    # 'speed_involved', 'driver_in_ext_distraction', 'driver_inattentive', 'driving_without_due_care', 'age_range', 'gender', 'vehicle_type', 'damage_location', 
    # 'damage_severity', 'pre_action', 'vehicle_body_style', 'vehicle_make', 'vehicle_model_year']
    #print(merged_df2.count()) #1082770

    #merged_df2.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in merged_df2.columns]).show()

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