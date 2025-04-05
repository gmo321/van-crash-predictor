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
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp
from delta import *
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import hour, round, col
from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import math
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, sum as _sum, isnan
from pyspark.sql.window import Window
from pyspark.sql.functions import mean, lit, desc


def main(spark):
    REAL_TIME_WEATHER_DATA_PATH_V1 = "s3a://van-crash-data/weather-data-delta/v1"
    REAL_TIME_WEATHER_DATA_PATH = "s3a://van-crash-data/weather-data-delta/"
    REAL_TIME_TRAFFIC_DATA_PATH = "s3a://van-crash-data/traffic-data-delta/"
    REAL_TIME_TRAFFIC_DATA_PATH_v1 = "s3a://van-crash-data/traffic-data-delta/v1"
    
    crash_df = spark.read.parquet("/Users/gloriamo/Desktop/van-crash-predictor/data/parquet/merged")
    weather_agg_df = spark.read.format("delta").load(REAL_TIME_WEATHER_DATA_PATH)
    weather_agg_df_v1 = spark.read.format("delta").load(REAL_TIME_WEATHER_DATA_PATH_V1)
    traffic_agg_df_v1 = spark.read.format("delta").load(REAL_TIME_TRAFFIC_DATA_PATH_v1)
    traffic_agg_df = spark.read.format("delta").load(REAL_TIME_TRAFFIC_DATA_PATH)
    
    #print(weather_agg_df.count()) 
    #print(weather_agg_df_v1.count())
    #print(traffic_agg_df.count())
    #print(traffic_agg_df_v1.count())
    

    crash_df = crash_df.withColumn("lat_bin", round(col("latitude"), 3)) \
                .withColumn("lon_bin", round(col("longitude"), 3)) \
                .drop('latitude') \
                .drop('longitude') \
                   
    weather_agg_df = weather_agg_df \
                .withColumn("hour", hour(col("window.start"))) \
                .withColumn("lat_bin", round(col("latitude"), 3)) \
                .withColumn("lon_bin", round(col("longitude"), 3)) \
                .drop('window') \
                .drop('latitude') \
                .drop('longitude') \
                    
                    
    traffic_agg_df = traffic_agg_df \
                .withColumn("hour", hour(col("window.start"))) \
                .withColumn("lat_bin", round(col("latitude"), 3)) \
                .withColumn("lon_bin", round(col("longitude"), 3)) \
                .drop('window') \
                .drop('latitude') \
                .drop('longitude') \
                    
    
    #print(weather_agg_df.count())
    #print(traffic_agg_df.count())
    #traffic_agg_df.show(truncate=False)
    #weather_agg_df_v1.show(truncate=False)
    #weather_agg_df.show() #name|avg_temp|avg_visibility|avg_clouds|max_rain|max_snow|last_weather|last_weather_description|hour|lat_bin|lon_bin
             
    def add_time_period(df):
        return df.withColumn("time_period",
            when((col("hour") >= 6) & (col("hour") <= 11), "morning")
            .when((col("hour") >= 12) & (col("hour") <= 17), "afternoon")
            .when((col("hour") >= 18) & (col("hour") <= 23), "evening")
            .otherwise("night")
        )

                                             
    weather_agg_df = add_time_period(weather_agg_df)
    traffic_agg_df = add_time_period(traffic_agg_df)
    
    weather_agg_df = weather_agg_df.groupBy("lat_bin", "lon_bin", "time_period") \
        .agg(
            F.avg("avg_temp").alias("avg_temp"),
            F.avg("avg_visibility").alias("avg_visibility"),
            F.avg("avg_clouds").alias("avg_clouds"),
            F.avg("max_rain").alias("max_rain"),
            F.avg("max_snow").alias("max_snow"),
            F.first("last_weather", ignorenulls=True).alias("last_weather"),
            F.first("last_weather_description", ignorenulls=True).alias("last_weather_description"),
        ).cache()


    traffic_agg_df = traffic_agg_df.groupBy("lat_bin", "lon_bin", "time_period") \
        .agg(
            F.avg("avg_speed").alias("avg_speed"),
            F.avg("avg_flow_speed").alias("avg_flow_speed"),
            F.avg("avg_travel_time").alias("avg_travel_time"),
            F.avg("avg_flow_travel_time").alias("avg_flow_travel_time"),
            F.sum("had_closure").alias("had_closure")
        ).cache()
        
    # Assign numerical weights to severity levels
    crash_df = crash_df.withColumn(
        "severity_weight",
        F.when(F.col("crash_severity") == "minor", 1)
        .when(F.col("crash_severity") == "moderate", 2)
        .when(F.col("crash_severity") == "severe", 3)
        .otherwise(0)
    )

    # ['latitude', 'longitude', 'year', 'month', 'day', 'municipality', 'region', 'total_casualty', 'is_intersection_crash', 'street', 'total_crashes', 
    # 'cross_street', 'weather', 'road_condition', 'road_surface', 'speed_limit_km_h', 'pedestrian_involved', 'total_vehicles_involved', 'distraction_involved', 
    # 'drug_involved', 'impaired_involved', 'speed_involved', 'is_weekend', 'time_period', 'is_rush_hour', 'season', 'crash_severity', 'lat_bin', 'lon_bin']

    # Group by lat-long and aggregate required fields
    crash_df = crash_df.groupBy("lat_bin", "lon_bin", "time_period") \
        .agg(
            F.sum("total_crashes").alias("total_crashes"),
            F.avg("severity_weight").alias("avg_crash_severity"),
            F.avg("speed_limit_km_h").alias("avg_speed_limit"),
            F.first("season", ignorenulls=True).alias("season"),
            F.first("road_condition", ignorenulls=True).alias("road_condition"),
            F.first("road_surface", ignorenulls=True).alias("road_surface"),
            F.first("weather", ignorenulls=True).alias("weather"),
            F.avg("total_vehicles_involved").alias("avg_total_vehicles_involved"),  
            F.avg("total_casualty").alias("avg_total_casualty"),
            F.avg("is_intersection_crash").cast("double").alias("pct_intersection_crash"),
            F.avg("pedestrian_involved").cast("double").alias("pct_pedestrian_involved"),
            F.avg("distraction_involved").cast("double").alias("pct_distraction_involved"),
            F.avg("drug_involved").cast("double").alias("pct_drug_involved"),
            F.avg("impaired_involved").cast("double").alias("pct_impaired_involved"),
            F.avg("speed_involved").cast("double").alias("pct_speed_involved"),
            F.avg("is_weekend").cast("double").alias("pct_is_weekend"),
            F.avg("is_rush_hour").cast("double").alias("pct_is_rush_hour"),
        ).cache()

        
    # Join real-time weather and traffic data because they have similar lat, lon
    weather_traffic_df = weather_agg_df.join(
        traffic_agg_df,
        on=["lat_bin", "lon_bin", "time_period"],
        how="inner")
    
    #weather_traffic_df = weather_traffic_df \
    #    .withColumnRenamed("lat_bin", "lat_bin_real") \
    #    .withColumnRenamed("lon_bin", "lon_bin_real") \
    #    .withColumnRenamed("time_period", "time_period_real")
        
    #print(weather_traffic_df.columns)  
    #print(weather_traffic_df.count())
    #weather_traffic_df.show()
    
    
    merged_df = crash_df.join(
        broadcast(weather_traffic_df),
        on=["lat_bin", "lon_bin", "time_period"],
        how="left"
    )
    
    #merged_df.show()
    
    #print(merged_df.count())

    '''
    # STEP 3: Join historical crash bins with weather_traffic based on Haversine distance
    # First, extract unique crash bins for matching
    crash_bins = crash_df.select("lat_bin", "lon_bin", "time_period").distinct()
    
    # Now join the crash data with weather_traffic_df to keep all columns from crash_df
    matched_bins = crash_bins.join(
        weather_traffic_df,
        on=["lat_bin", "lon_bin", "time_period"],
        how="left"
    )
    
    # Merge matched_bins back with the original crash_df to retain all crash-related data
    final_matched_bins = crash_df.join(
        matched_bins,
        on=["lat_bin", "lon_bin", "time_period"],
        how="inner"
    )
    
    # Define Haversine UDF
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371  # Radius of the Earth in km
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        
        a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c  # Distance in km
    
    # Register UDF
    haversine_udf = udf(haversine, DoubleType())

    # Compute distance
    final_matched_bins = final_matched_bins.withColumn(
        "distance_km",
        haversine_udf(
            col("lat_bin"), col("lon_bin"),  # from crash bin
            col("lat_bin"), col("lon_bin") 
        )
    )

    # Filter matches within distance threshold (e.g. 1km)
    final_matched_bins = final_matched_bins.filter(col("distance_km") <= 1)
    
    # Window by crash bin (lat_bin, lon_bin, time_period), ordered by closest distance
    windowSpec = Window.partitionBy("lat_bin", "lon_bin", "time_period").orderBy(col("distance_km").asc())

    # Rank each candidate, keep only the closest match
    final_matched_bins_dedup = final_matched_bins.withColumn("rank", F.row_number().over(windowSpec)) \
                                    .filter(col("rank") == 1) \
                                    .drop("rank")
    '''
    
                                    
    #matched_bins_dedup.show()
    
    # Choose only needed columns to avoid duplication
    weather_traffic_clean = weather_traffic_df.select(
        col("lat_bin"),
        col("lon_bin"),
        col("time_period"),
        col("avg_temp"),
        col("avg_visibility"),
        col("avg_clouds"),
        col("max_rain"),
        col("max_snow"),
        col("last_weather"),
        col("last_weather_description"),
        col("avg_speed"),
        col("avg_flow_speed"),
        col("avg_travel_time"),
        col("avg_flow_travel_time")
    )
    '''
     col("had_closure"),
        col("avg_crash_severity"),
        col("avg_speed_limit"),
        col("season"),
        col("road_condition"),
        col("road_surface"),
        col("weather"),
        col("avg_total_vehicles_involved"),
        col("avg_total_casualty"),
        col("pct_intersection_crash"),
        col("pct_pedestrian_involved"),
        col("pct_distraction_involved"),
        col("pct_drug_involved"),
        col("pct_impaired_involved"),
        col("pct_speed_involved"),
        col("pct_is_weekend"),
        col("pct_is_rush_hour"),
    '''
    # Drop weather/traffic columns from matched_bins_dedup before join
    columns_to_drop = [
        "avg_temp", "avg_visibility", "avg_clouds", "max_rain", "max_snow",
        "last_weather", "last_weather_description", "avg_speed", "avg_flow_speed",
        "avg_travel_time", "avg_flow_travel_time", "distance_km", "had_closure"
    ]

    #matched_bins_dedup_clean = final_matched_bins_dedup.drop(*columns_to_drop)

    # Now join matched_bins with agg_df on (lat_bin, lon_bin, time_period) and add the weather/traffic features
    #final_df = matched_bins_dedup_clean.join(
    #    broadcast(weather_traffic_clean),  # drop distance if not needed
    #    on=["lat_bin", "lon_bin", "time_period"],
    #    how="left"
    #)
    
    
    merged_df = merged_df.dropDuplicates()

    merged_df = merged_df.repartition(100)
    #print(final_df.columns)
    #['lat_bin_real', 'lon_bin_real', 'time_period_real', 'lat_bin', 'lon_bin', 'time_period', 'avg_visibility', 'avg_clouds', 
    # 'max_rain', 'max_snow', 'last_weather', 'last_weather_description', 'avg_speed', 'avg_flow_speed', 'avg_travel_time', 
    # 'avg_flow_travel_time', 'distance_km', 'avg_visibility', 'avg_clouds', 'max_rain', 'max_snow', 'last_weather', 
    # 'last_weather_description', 'avg_speed', 'avg_flow_speed', 'avg_travel_time', 'avg_flow_travel_time']
    
    
            
        
    #final_df = final_df.drop('lat_bin_real', 'lon_bin_real', )
    
    #null_counts = merged_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in merged_df.columns])

    #null_counts.show()
    #avg_temp|avg_visibility|avg_clouds|max_rain|max_snow|last_weather|last_weather_description|avg_speed|avg_flow_speed|avg_travel_time|avg_flow_travel_time|had_closure|
    
    #null_columns = [c for c in merged_df.columns if merged_df.filter(col(c).isNull()).count() > 0]

    #print("Columns with null values:", null_columns)

    # Impute numeric columns with mean
    numeric_columns = ['avg_temp', 'avg_visibility', 'avg_speed', 'avg_clouds', 'max_rain', 'max_snow', 'avg_flow_speed', 'avg_travel_time', 'avg_flow_travel_time', 'had_closure']
    def impute_numeric_column(df, column):
        """Safely impute numeric column with mean, handling null means"""
        mean_value = df.select(mean(when(col(column).isNotNull(), col(column)))).first()[0]
        
        # If mean is null (all values in column are null), use 0
        if mean_value is None:
            print(f"Warning: Mean value for column {column} is null. Using 0 instead.")
            mean_value = 0
            
        return df.withColumn(
            column,
            when(col(column).isNull(), mean_value).otherwise(col(column))
        )

    # For categorical columns, you can use mode
    def impute_categorical_column(df, column, default_value="Unknown"):
        """Safely impute categorical column with mode, handling null modes"""
        # Get value counts for non-null values
        value_counts = df.filter(col(column).isNotNull()).groupBy(column).count()
        
        # Check if we have any non-null values
        if value_counts.count() > 0:
            # Get the mode (most frequent value)
            mode_row = value_counts.orderBy(desc("count")).first()
            mode_value = mode_row[0]
            
            # Use mode_value for imputation
            return df.withColumn(
                column,
                when(col(column).isNull(), mode_value).otherwise(col(column))
            )
        else:
            # All values are null, use default
            print(f"Warning: All values in column {column} are null. Using default value '{default_value}'.")
            return df.withColumn(column, lit(default_value))

    categorical_columns = ['last_weather', 'last_weather_description']
    
    for column in numeric_columns:
        merged_df = impute_numeric_column(merged_df, column)     
        
    # Apply imputation for categorical columns
    categorical_columns = ['last_weather', 'last_weather_description']
    for column in categorical_columns:
        merged_df = impute_categorical_column(merged_df, column)

    # Verify imputation worked
    #null_counts = merged_df.select([
    #    sum(col(c).isNull().cast("int")).alias(c) for c in numeric_columns + categorical_columns
    #])
    
    #print("Remaining null counts after imputation:")
    #null_counts.show(truncate=False)
    
    merged_df = merged_df \
        .withColumn("avg_temp", round("avg_temp", 2)) \
        .withColumn("avg_speed", round("avg_speed", 2)) \
        .withColumn("avg_visibility", round("avg_visibility", 0)) \
        .withColumn("avg_clouds", round("avg_clouds", 1)) \
        .withColumn("avg_travel_time", round("avg_travel_time", 2)) \
        .withColumn("had_closure", round("had_closure", 2)) \
        .withColumn("avg_crash_severity", round("avg_crash_severity", 2)) \
        .withColumn("avg_speed_limit", round("avg_speed_limit", 2)) \
        .withColumn("avg_total_vehicles_involved", round("avg_total_vehicles_involved", 2)) \
        .withColumn("avg_total_casualty", round("avg_total_casualty", 2)) \
        .withColumn("pct_intersection_crash", round("pct_intersection_crash", 2)) \
        .withColumn("pct_pedestrian_involved", round("pct_pedestrian_involved", 2)) \
        .withColumn("pct_distraction_involved", round("pct_distraction_involved", 2)) \
        .withColumn("pct_drug_involved", round("pct_drug_involved", 2)) \
        .withColumn("pct_impaired_involved", round("pct_impaired_involved", 2)) \
        .withColumn("pct_speed_involved", round("pct_speed_involved", 2)) \
        .withColumn("pct_is_weekend", round("pct_is_weekend", 2)) \
        .withColumn("pct_is_rush_hour", round("pct_is_rush_hour", 2)) \
        .withColumn("avg_flow_speed", round("avg_flow_speed", 2)) \
        .withColumn("avg_flow_travel_time", round("avg_flow_travel_time", 2)).cache()
    
    #merged_df.show()
    #print(merged_df.count()) #25871
    
    
    #print(traffic_agg_df.select("lat_bin", "lon_bin", "time_period").distinct().count())
    #print(weather_agg_df.select("lat_bin", "lon_bin", "time_period").distinct().count())
    #print(weather_traffic_df.select("lat_bin_real", "lon_bin_real", "time_period_real").distinct().count())
    #print(final_df.select("lat_bin", "lon_bin", "time_period_real").distinct().count())
    
    #print("Matched bins count:", matched_bins_dedup.count())
    #print("Weather+Traffic rows count:", weather_traffic_df.count())
    #print("Final joined count:", final_df.count())
    
    
    #print(final_df.count()) #3170 04-04

    #null_counts = final_df.select([
    #    _sum(col(c).isNull().cast("int")).alias(c) for c in final_df.columns
    #])
    
    #null_counts.show()
    # Optionally, save locally for debugging
    merged_df.write.parquet("data/parquet/final_real_df", compression="snappy", mode='overwrite')
    
    return


if __name__ == '__main__':  
    builder = SparkSession.builder \
        .appName('Real-Time Data Processing') \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:jar:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    # Configure the Spark session with Delta using Delta Pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    main(spark)