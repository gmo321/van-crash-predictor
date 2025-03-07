import requests
import logging
import os


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
        
def main():
    address = "Vancouver, BC"
    geocode_data(address)
    
if __name__ == '__main__':
    main()