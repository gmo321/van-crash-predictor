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
                        .csv("s3a://van-crash-data/icbc/raw/ICBC_BC_full.csv", schema=df_schema)
                        

    #icbc_df.show(1)
    
    
    # Drop unnecessary columns
    icbc_df = icbc_df.drop('Crash Breakdown 2', 'Municipality With Boundary', 'Animal Flag', 'Cyclist Flag', 'Heavy Vehicle Flag', 'Motorcycle Flag', \
        'Parked Vehicle Flag', 'Parking Lot Flag', 'Pedestrian Flag', 'Metric Selector', 'Municipality Name (ifnull)', 'Street Full Name', "Road Location Description")
    
    # Columns
    #['Date Of Loss Year', 'Crash Severity', 'Day Of Week', 'Derived Crash Configuration', 'Intersection Crash', 'Month Of Year', 
    # 'Region', 'Street Full Name (ifnull)', 'Time Category', 'Municipality Name', 'Total Crashes', 'Total Victims', 'Latitude', 
    # 'Cross Street Full Name', 'Longitude', 'Mid Block Crash', 'is_duplicate']
    
    #icbc_df.select('Cross Street Full Name', 'Street Full Name (ifnull)').show(truncate=False)
    
    icbc_df = icbc_df.withColumnsRenamed({'Date Of Loss Year': 'year', 
                                          'Crash Severity': 'crash_severity', 
                                          'Day Of Week': 'day', 
                                          'Derived Crash Configuration': 'crash_config',
                                          'Intersection Crash': 'is_intersection_crash', 
                                          'Month Of Year': 'month',
                                          'Region':'region', 
                                          'Street Full Name (ifnull)': 'street',
                                          'Time Category': 'time',
                                          'Municipality Name': 'municipality',
                                          'Total Crashes':'total_crashes',
                                          'Total Victims':'total_victims',
                                          'Latitude':'latitude',
                                          'Cross Street Full Name':'cross_street',
                                          'Longitude': 'longitude',
                                          'Mid Block Crash': 'is_mid_block'})
    
    
    # Create 'city' column
    icbc_df = icbc_df.withColumn('city', concat_ws(', ', icbc_df['street'], icbc_df['municipality']))
    
    #icbc_df = icbc_df.withColumn('city', concat(icbc_df['city'], lit(', BC')))
    #icbc_df.show(truncate=False)
    
    
    #print(lon_lat_null_df.count())     # Rows where cross_street is null but not lon, lat: 517147  
    # Number of null lon, lat rows: 270063
    # Number of distinct cities and lat, lon is NULL: 54340 --> Only need to geocode 54340
    
    # Generate unique ID as key column
    #icbc_df = icbc_df.withColumn('id', monotonically_increasing_id())
    
    # TODO check nulls, duplicate rows
    #icbc_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in icbc_df.columns]).show()
    
    #total_rows = icbc_df.count()
    #print(f'Total rows: {total_rows}')
    #Total rows: 1360159

    
    #icbc_df.write.options(compression='LZ4', mode='overwrite').parquet("parquet/icbc")
    
    DAILY_LIMIT = 40000
    request_counter = 0
    
    def geocode_data(address):
        API_KEY = 'AIzaSyAz_l8je7Wp1Vxd3PG7apLagAys4MOd9v4'
        url = 'https://maps.googleapis.com/maps/api/geocode/json?parameters'
        if not address:
            return None, None
        
        params = {
            'key': API_KEY,
            'address': address,
            'components': 'country:CA'
            
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            response_json = response.json()
            
            if response_json.get('status') == 'OK' and response_json.get('results'):
            #print(response_json)
                lat = response_json.get('results', {})[0].get('geometry', 'Unknown').get('location', 'Unknown').get('lat', 'No latitude available')
                lon = response_json.get('results', {})[0].get('geometry', 'Unknown').get('location', 'Unknown').get('lng', 'No longitude available')
                return lat, lon
            else:
                logging.warning(f"No results found for address: {address}")
                return None, None
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching weather data: {e}")
            return None, None
    
    # Define UDFs for geocoding
    @pandas_udf(DoubleType())
    def geocode_lat_udf(addresses: pd.Series) -> pd.Series:
        return addresses.apply(lambda x: geocode_data(x)[0])
    
    @pandas_udf(DoubleType())
    def geocode_lon_udf(addresses: pd.Series) -> pd.Series:
        return addresses.apply(lambda x: geocode_data(x)[1])
    
    # Filter rows with missing latitude and longitude
    missing_lat_lon_df = icbc_df.filter((icbc_df['latitude'].isNull()) | (icbc_df['longitude'].isNull()))
    
    # Extract distinct cities
    distinct_cities_df = missing_lat_lon_df.select('city').distinct()
    
    distinct_cities_df = distinct_cities_df.withColumn('id', monotonically_increasing_id())
    
    # Process in batches
    batch_size = 1000
    total_rows = distinct_cities_df.count()
    batches = (total_rows // batch_size) + 1
    
    for batch_num in range(batches):
        # Check if the daily limit has been reached
        if request_counter >= DAILY_LIMIT:
            logger.warning(f"Daily limit of {DAILY_LIMIT} requests reached. Stopping processing.")
            break
    
        start_idx = batch_num * batch_size
        end_idx = start_idx + batch_size
        
        # Filter the current batch using the 'id' column
        batch_df = distinct_cities_df.filter((col("id") >= start_idx) & (col("id") < end_idx))
        
        # Geocode the batch
        batch_df = batch_df.withColumn("latitude", geocode_lat_udf(batch_df['city'])) \
                           .withColumn("longitude", geocode_lon_udf(batch_df['city']))
        
        # Increment the request counter by the number of rows in the batch
        request_counter += batch_df.count()
    
        # Write the batch to output
        batch_df.write.mode("append").parquet("/Users/gloriamo/Desktop/van-crash-predictor/data/geocoded")
        
        logger.info(f"Processed batch {batch_num + 1}/{batches} with {batch_df.count()} rows")
        logger.info(f"Total requests made so far: {request_counter}")
        
        # Sleep to respect API rate limits
        time.sleep(5)
    
    # Log the total number of requests made
    logger.info(f"Total requests made: {request_counter}")
    
    # Read all geocoded cities
    geocoded_cities_df = spark.read.parquet("/Users/gloriamo/Desktop/van-crash-predictor/data/geocoded")
    
    geocoded_cities_df.show(truncate=False)
    
    
    
    
    # Combine Street Name and municipality
    #df = icbc_df.select(concat_ws(',', icbc_df['Street Full Name (ifnull)'], icbc_df['Municipality Name'])
    #                    .alias('city'), 'Latitude', 'Longitude')
    
    # Get distinct cities where lat/lon is missing
    #lon_lat_null_df = df.filter(df['Latitude'].isNull() | df['Longitude'].isNull()).select('city').distinct()
    
    #lon_lat_null_df.show(truncate=False)
    
    #total_rows = lon_lat_null_df.count()
    #print(f'Total rows lon, lat null: {total_rows}')
    #54340 NULL
    
    # TODO check null lon, lat columns
    # perform geocoder on null lon, lat columns

    
    
    


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('icbc etl') \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(spark)