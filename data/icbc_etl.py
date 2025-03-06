import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import os
import json

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id, concat_ws, col, lit, udf


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
                        .csv("s3a://van-crash-data/icbc/raw/ICBC_BC_full.csv", schema=df_schema)
                        

    #icbc_df.show(1)
    
    # Checking for duplicate columns
    icbc_df = icbc_df.withColumn('is_duplicate', 
                                 F.when(F.col('Street Full Name') == F.col('Street Full Name (ifnull)'), F.lit(True))
                                 .otherwise(F.lit(False)))
    
    diff_rows = icbc_df.filter(F.col('is_duplicate') == False).select('Street Full Name', 'Street Full Name (ifnull)')
    
    
    # Drop unnecessary columns
    icbc_df = icbc_df.drop('Crash Breakdown 2', 'Municipality With Boundary', 'Animal Flag', 'Cyclist Flag', 'Heavy Vehicle Flag', 'Motorcycle Flag', \
        'Parked Vehicle Flag', 'Parking Lot Flag', 'Pedestrian Flag', 'Metric Selector', 'Municipality Name (ifnull)', 'Cross Street Full Name', 'Street Full Name')
    

    #icbc_df.select('Street Full Name (ifnull)', "Municipality Name", "Region").show(truncate=False)
    #icbc_df.select('Crash Severity', 'Street Full Name', 'Month Of Year').show(truncate=False)
    
    # Generate unique ID as key column
    icbc_df = icbc_df.withColumn('id', monotonically_increasing_id())
    
    # TODO check nulls, duplicate rows
    #icbc_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in icbc_df.columns]).show()
    
    #total_rows = icbc_df.count()
    #print(f'Total rows: {total_rows}')
    #Total rows: 1360159

    
    #icbc_df.write.options(compression='LZ4', mode='overwrite').parquet("parquet/icbc")
    
    '''
    cache_file = "geocoded_cache.json"
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            cache = json.load(f)
    else:
        cache = {}
    '''
    
    '''
    def geocode_function(city):
        geolocator = Nominatim(user_agent="myGeocoder")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)
        time.sleep(2)
        location = geocode(city)
        
        if location:
            return (location.latitude, location.longitude)
        else:
            return (None, None)  
   
        
    geocode_udf = udf(geocode_function, StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]))
                

    # TODO keep as spark df, add city column
    df = icbc_df.select(concat_ws(',', icbc_df['Street Full Name (ifnull)'], icbc_df['Municipality Name'])
                        .alias('city'), 'Latitude', 'Longitude')
    
    lon_lat_null_df = df.filter(df['Latitude'].isNull() | df['Longitude'].isNull())
    
    city_df = lon_lat_null_df.withColumn("coordinates", geocode_udf(lon_lat_null_df['city']))
    
    city_df.show(truncate=False)
    '''
    
    
    #city_df.show(truncate=False)
    
    
    
    #df = icbc_df.toPandas()
    #df['city'] = df['Street Full Name (ifnull)'] + ',' + df['Municipality Name']
    #print(df['city'])
    
    #print(df.columns)
    
    #lon_lat_null_df = df.loc[df['Latitude'].isnull() | df['Longitude'].isnull()]
    
    #print(len(lon_lat_null_df.index))
    
    # TODO check null lon, lat columns
    # perform geocoder on null lon, lat columns

    
    
    #lon_lat_null_df['location'] = lon_lat_null_df['city'].progress_apply(geocode)
    #lon_lat_null_df['point'] = lon_lat_null_df['location'].apply(lambda loc: tuple(loc.point) if loc else None)
    
    
    
    


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('icbc etl') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)