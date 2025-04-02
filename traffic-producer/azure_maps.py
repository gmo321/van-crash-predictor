import requests
import logging
import csv
import json
import concurrent.futures
import os
import pandas as pd
from dotenv import load_dotenv
import time
import schedule 

load_dotenv()

logging.basicConfig(level=logging.WARNING)

api_key = os.getenv("TRAFFIC_API_KEY")
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
    "zoom": 100,
    'unit': 'kmph',
    'thickness': 10,
    'openLr': 'false',
    'jsonp': 'false'
    }

    try: 
        response = requests.get(url, params=params)
        response.raise_for_status()
        if response.status_code == 429:  # If rate limit is exceeded
            reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
            sleep_time = reset_time - time.time() + 1  
            print(f"Rate limit hit, sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)
            return fetch_traffic_data(point)  # Retry after sleep
    
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
            error_message = e.response.json().get("error", "")
            if error_message == "Point too far from nearest existing segment.":
                return None 
        logging.error(f"HTTP Error for {point}: {e}")
        return None
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching traffic data for {point}: {e}")
        return None
    
   

def fetch_bulk_data(points):
    """Fetch traffic data for all points concurrently."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_traffic_data, points))
    #return results
    return {"traffic_data": [res for res in results if res]}


# Schedules task every 30 minutes
def schedule_polling():
    logging.info("Starting scheduled polling...")
    schedule.every(30).minutes.do(main)  # Schedules the `main()` function every 30 minutes

    while True:
        schedule.run_pending()  # Run all the pending scheduled tasks
        time.sleep(1)

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
    schedule_polling()
