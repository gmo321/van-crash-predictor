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
from pyspark.sql.functions import hour, round, col, day
from pyspark.sql.functions import when
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import math
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, sum as _sum, isnan
from pyspark.sql.window import Window
from pyspark.sql.functions import mean, lit, desc


def main(spark):
    REAL_TIME_WEATHER_DATA_PATH = "s3a://van-crash-data/weather-data-delta/"
    REAL_TIME_TRAFFIC_DATA_PATH = "s3a://van-crash-data/traffic-data-delta/"
    
    crash_df = spark.read.parquet("/Users/gloriamo/Desktop/van-crash-predictor/data/parquet/merged")
    weather_agg_df = spark.read.format("delta").load(REAL_TIME_WEATHER_DATA_PATH)
    traffic_agg_df = spark.read.format("delta").load(REAL_TIME_TRAFFIC_DATA_PATH)
    
    #traffic_agg_df.show(truncate=False)
    #traffic_agg_df.orderBy(col("window").getField("end").desc()).show(truncate=False)
    
    #weather_agg_df.orderBy(col("window").getField("end").desc()).show(truncate=False)
    #print(weather_agg_df.count()) 
    #8947
    #print(weather_agg_df_v1.count())
    #print(traffic_agg_df.count())
    #8802
    #print(traffic_agg_df_v1.count())

    crash_df = crash_df.withColumn("lat_bin", round(col("latitude"), 3)) \
                .withColumn("lon_bin", round(col("longitude"), 3)) \
                .drop('latitude') \
                .drop('longitude') \

                    
    print(crash_df.columns)
                   
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
                .withColumn("avg_speed_diff", col("avg_speed") - col("avg_flow_speed")) \
                .drop('window') \
                .drop('latitude') \
                .drop('longitude') \
    
    #print(weather_agg_df.columns)
    #print(traffic_agg_df.columns)
    #traffic_agg_df.show(truncate=False)
                    
    #print(weather_agg_df.count()) #7668
    #print(traffic_agg_df.count()) #5984
             
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
            F.avg("avg_speed_diff").alias("avg_speed_diff"),
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
    
    #print(crash_df.columns)


    # Group by lat-long and aggregate required fields
    crash_df = crash_df.groupBy("lat_bin", "lon_bin", "time_period") \
        .agg(
            F.first("year").alias("year"),
            F.first("month").alias("month"),
            F.first("day").alias("day"),
            F.first("municipality").alias("municipality"),
            F.first("region").alias("region"),
            F.sum("total_crashes").alias("total_crashes"),
            F.first("crash_severity").alias("crash_severity"),
            F.avg("severity_weight").alias("avg_severity_weight"),
            F.first("weather", ignorenulls=True).alias("weather"),
            F.avg("speed_limit_km_h").alias("avg_speed_limit"),
            F.first("season", ignorenulls=True).alias("season"),
            F.first("road_condition", ignorenulls=True).alias("road_condition"),
            F.first("road_surface", ignorenulls=True).alias("road_surface"),
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
    
    #crash_df.groupBy("crash_severity").count().show()
    #crash_df.describe("avg_severity_weight").show()

    
    # Join real-time weather and traffic data because they have similar lat, lon
    weather_traffic_df = weather_agg_df.join(
        traffic_agg_df,
        on=["lat_bin", "lon_bin", "time_period"],
        how="inner")

    
    merged_df = crash_df.join(
        broadcast(weather_traffic_df),
        on=["lat_bin", "lon_bin", "time_period"],
        how="left"
    )
    
    #merged_df.show()
    
    #print(merged_df.count())
    
    
    merged_df = merged_df.dropDuplicates()

            
    
    # Impute numeric columns with mean
    numeric_columns = ['avg_temp', 'avg_visibility', 'avg_speed', 'avg_clouds', 'max_rain', 'max_snow', 'avg_flow_speed', 'avg_travel_time', 'avg_flow_travel_time', 'avg_speed_diff', 'had_closure']
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

    for column in numeric_columns:
        merged_df = impute_numeric_column(merged_df, column)    
    
    
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
        
        
    # Apply imputation for categorical columns
    categorical_columns = ['last_weather', 'last_weather_description']
    for column in categorical_columns:
        merged_df = impute_categorical_column(merged_df, column)

    # Verify imputation worked
    #null_counts = merged_df.select([
    #    sum(col(c).isNull().cast("int")).alias(c) for c in numeric_columns
    #])
    
    #print("Remaining null counts after imputation:")
    #null_counts.show(truncate=False)
    
    #merged_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in merged_df.columns]).show()
    
    merged_df = merged_df \
        .withColumn("avg_temp", round("avg_temp", 2)) \
        .withColumn("avg_speed", round("avg_speed", 2)) \
        .withColumn("avg_visibility", round("avg_visibility", 0)) \
        .withColumn("avg_clouds", round("avg_clouds", 1)) \
        .withColumn("avg_travel_time", round("avg_travel_time", 2)) \
        .withColumn("had_closure", round("had_closure", 2)) \
        .withColumn("avg_speed_limit", round("avg_speed_limit", 2)) \
        .withColumn("avg_severity_weight", round("avg_severity_weight", 2)) \
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
        
    
    #print(merged_df.count()) #25871
    
    #null_counts = merged_df.select([
    #    _sum(col(c).isNull().cast("int")).alias(c) for c in merged_df.columns
    #])
    
    #null_counts.show()
    #merged_df.show()
    # Save dataframe
    
    merged_df.write.parquet("data/parquet/final_real_df", compression="snappy", mode='overwrite')
    


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