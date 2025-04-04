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

def main(spark):
    REAL_TIME_WEATHER_DATA_PATH = "s3a://van-crash-data/weather-data-delta/v1/"
    REAL_TIME_TRAFFIC_DATA_PATH = "s3a://van-crash-data/traffic-data-delta/v1/"
    df = spark.read.parquet(...)
    
    
    weather_agg_df = spark.read.format("delta").load(REAL_TIME_WEATHER_DATA_PATH)
    traffic_agg_df = spark.read.format("delta").load(REAL_TIME_TRAFFIC_DATA_PATH)

    
    
    
    #weather_df = weather_df.withColumn("date", col("date").cast("timestamp"))
    
    weather_agg_df.show()
    
    #traffic_df_sorted = weather_df.orderBy(col("date").desc())
    
    #traffic_df_sorted.show()
    
    #real_time_df = traffic_df_sorted.withColumn('local_date', from_utc_timestamp(col('date'), 'PST'))

    #real_time_df.show(5)
    
    # Optionally, save locally for debugging
    #real_time_df.write.mode("overwrite").parquet("./data/real_time_sample.parquet")
    

    
    return


if __name__ == '__main__':  
    packages = [
        "io.delta:delta-spark_2.12:jar:3.2.0",
    ]
    
    #delta_spark_jar_path = "/Users/gloriamo/Desktop/van-crash-predictor/spark_jars/delta-spark_2.12-3.2.0.jar"
    
    builder = SparkSession.builder \
        .appName('Real-Time Data Processing') \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:jar:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
    # Configure the Spark session with Delta using Delta Pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    
        
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    
    main(spark)