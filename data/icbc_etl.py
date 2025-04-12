import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

#from geopy.geocoders import Nominatim
#from geopy.extra.rate_limiter import RateLimiter
#from geopy.exc import GeocoderTimedOut
import time
import os
import json
import requests
import logging

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id, concat_ws, col, lit, udf, broadcast, concat, pandas_udf


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(spark):
    #path = 'raw/ICBC_BC_full.csv'

    df_schema = types.StructType([
    StructField("Crash Breakdown 2", StringType(), True),
    StructField("Date Of Loss Year", IntegerType(), True),
    StructField("Animal Flag", StringType(), True),
    StructField("Crash Severity", StringType(), True),
    StructField("Cyclist Flag", StringType(), True),
    StructField("Day Of Week", StringType(), True),
    StructField("Derived Crash Configuration", StringType(), True),
    StructField("Heavy Vehicle Flag", StringType(), True),
    StructField("Intersection Crash", StringType(), True),
    StructField("Month Of Year", StringType(), True),
    StructField("Motorcycle Flag", StringType(), True),
    StructField("Municipality Name (ifnull)", StringType(), True),
    StructField("Parked Vehicle Flag", StringType(), True),
    StructField("Parking Lot Flag", StringType(), True),
    StructField("Pedestrian Flag", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Street Full Name (ifnull)", StringType(), True),
    StructField("Time Category", StringType(), True),
    StructField("Municipality Name", StringType(), True),
    StructField("Road Location Description", StringType(), True),
    StructField("Street Full Name", StringType(), True),
    StructField("Metric Selector", IntegerType(), True),
    StructField("Total Crashes", IntegerType(), True),
    StructField("Total Victims", IntegerType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Cross Street Full Name", StringType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Mid Block Crash", StringType(), True),
    StructField("Municipality With Boundary", StringType(), True)
])
    
    
    icbc_df = spark.read.option("header", True) \
                        .option("sep", "\t") \
                        .option("encoding", "UTF-16") \
                        .csv("s3a://van-crash-data/icbc/raw/ICBC_BC_full.csv", schema=df_schema).repartition(100)
    
    
    # Drop unnecessary columns
    icbc_df = icbc_df.drop('Crash Breakdown 2', 'Municipality With Boundary', 'Animal Flag', 'Cyclist Flag', 'Heavy Vehicle Flag', 'Motorcycle Flag', \
        'Parked Vehicle Flag', 'Parking Lot Flag', 'Metric Selector', 'Mid Block Crash', 'Municipality Name (ifnull)', 'Street Full Name', "Road Location Description")
    
    
    icbc_df = icbc_df.withColumnsRenamed({'Date Of Loss Year': 'year', 
                                          'Crash Severity': 'crash_severity', 
                                          'Day Of Week': 'day', 
                                          'Derived Crash Configuration': 'crash_configuration',
                                          'Intersection Crash': 'is_intersection_crash', 
                                          'Month Of Year': 'month',
                                          'Region':'region', 
                                          'Street Full Name (ifnull)': 'street',
                                          'Time Category': 'time',
                                          'Pedestrian Flag': 'pedestrian_flag',
                                          'Municipality Name': 'municipality',
                                          'Total Crashes':'total_crashes',
                                          'Total Victims':'total_victims',
                                          'Cross Street Full Name': 'cross_street',
                                          'Latitude':'latitude',
                                          'Longitude': 'longitude'})
    
    
    # Create 'city' column
    icbc_df = icbc_df.withColumn('city', concat_ws(', ', icbc_df['street'], icbc_df['municipality']))


    #icbc_df.write.parquet("data/parquet/icbc", compression='LZ4', mode='overwrite')
    
    # Parquet data
    icbc_df.write.parquet("s3a://van-crash-data/icbc/processed-data/", compression='LZ4', mode='overwrite')
    
    
    


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('icbc etl') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)