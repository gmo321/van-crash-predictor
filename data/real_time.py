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

def main(spark):
    REAL_TIME_WEATHER_DATA_PATH = "s3a://van-crash-data/weather-data/"
    REAL_TIME_TRAFFIC_DATA_PATH = "s3a://van-crash-data/traffic-data/"

    traffic_df = spark.read.parquet(REAL_TIME_TRAFFIC_DATA_PATH)
    weather_df = spark.read.parquet(REAL_TIME_WEATHER_DATA_PATH)
    
    weather_df = weather_df.withColumn("date", col("date").cast("timestamp"))
    
    traffic_df_sorted = weather_df.orderBy(col("date").desc())
    
    #traffic_df_sorted.show()
    
    real_time_df = traffic_df_sorted.withColumn('local_date', from_utc_timestamp(col('date'), 'PST'))

    real_time_df.show(5)
    
    # Optionally, save locally for debugging
    #real_time_df.write.mode("overwrite").parquet("./data/real_time_sample.parquet")
    
    return


if __name__ == '__main__':  
    spark = SparkSession.builder \
        .appName('Real-Time Data Processing') \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)