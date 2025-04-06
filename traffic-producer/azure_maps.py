import requests
import logging
import csv
import json
import concurrent.futures
import os
import pandas as pd
from dotenv import load_dotenv
import time

load_dotenv()

logging.basicConfig(level=logging.WARNING)

api_key = "CSEZtSHXVFkJ72aGgxlT1AICfius7Pnz"
url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

sample_point = "49.887,-119.495"

def read_coordinates_from_csv():
    """Read longitude and latitude from the CSV file."""
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    FILE_PATH = os.path.join(BASE_DIR, "city_list.csv")
    
    coordinates = []
    
    with open(FILE_PATH, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            latitude = row.get('Y')
            longitude = row.get('X')
            if longitude and latitude:
                coordinates.append(f"{latitude},{longitude}") 
    return coordinates

    #coordinates = []
    #with open(FILE_PATH, mode='r') as file:
    #    reader = csv.DictReader(file)
    #    for row in reader:
    #        longitude = row.get('longitude')
    #        latitude = row.get('latitude')
    #        if longitude and latitude:
    #            coordinates.append(f"{latitude},{longitude}") 
    #return coordinates

def fetch_traffic_data(point):
    params = {
    'key': api_key,
    'point': point,
    'unit': 'kmph',
    'thickness': 10,
    'openLr': 'false',
    'jsonp': 'false'
    }
    
    max_retries = 5  # Limit retries to avoid infinite loops
    retry_delay = 10  # Initial delay in seconds for exponential backoff
    
    for attempt in range(max_retries):
        try: 
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            if response.status_code == 429:  # Too many requests (rate limit)
                reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
                sleep_time = max(reset_time - time.time(), retry_delay)
                logging.warning(f"Rate limit exceeded (Attempt {attempt+1}/{max_retries}). Sleeping for {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)
                retry_delay *= 2  # Exponential backoff (10s -> 20s -> 40s ...)
                continue 

            data = response.json()
            
            # Check if the response contains an error about the point being too far
            if "error" in data and data.get("error") == "Point too far from nearest existing segment.":
                return None  # Skip this point silently
            
            segment_data = data.get("flowSegmentData", {})
            if not segment_data:
                return None

            return {
                "latitude": float(point.split(",")[0]),
                "longitude": float(point.split(",")[1]),
                "current_speed": segment_data.get("currentSpeed", "N/A"),
                "free_flow_speed": segment_data.get("freeFlowSpeed", "N/A"),
                "current_travel_time": segment_data.get("currentTravelTime", "N/A"),
                "free_flow_travel_time": segment_data.get("freeFlowTravelTime", "N/A"),
                "confidence": segment_data.get("confidence", "N/A"),
                "road_closure": segment_data.get("roadClosure", "N/A"),
                "date": response.headers.get("Date", "N/A")
            }
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                logging.info(f"Skipping point {point}: Received 400 Bad Request.")
                return None  # Skip and do not retry
        
        
   

def fetch_bulk_data(points):
    """Fetch traffic data for all points concurrently."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_traffic_data, points))
    #return results
    return {"traffic_data": [res for res in results if res]}


def main():
    logging.info("Starting data polling process.")
    coordinates = read_coordinates_from_csv()
    #response = fetch_traffic_data(sample_point)
    #if response:
    #    print("Traffic Data Response:")
    #    print(json.dumps(response, indent=4))
    #else:
    #    print("No traffic data received or an error occurred.")
        
    #print(response)
    data = fetch_bulk_data(coordinates)

    #print(json.dumps(data, indent=4))
    with open("traffic_data_bc_sites.json", "w") as file:
        json.dump(data, file, indent=4)
        
    return data
        

if __name__ == "__main__":
    main()
