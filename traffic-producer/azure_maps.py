import requests
import logging
import csv
import json
import concurrent.futures
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.WARNING)

api_key = os.getenv("TRAFFIC_API_KEY")
url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

sample_point = "49.2827,-123.1207"

def read_coordinates_from_csv():
    """Read longitude and latitude from the CSV file."""
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    FILE_PATH = os.path.join(BASE_DIR, "bc_sites2.csv")

    coordinates = []
    with open(FILE_PATH, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            longitude = row.get('longitude')
            latitude = row.get('latitude')
            if longitude and latitude:
                coordinates.append(f"{latitude},{longitude}") 
    return coordinates

def fetch_traffic_data(point):
    params = {
    'key': api_key,
    'point': point,
    'unit': 'KMPH',
    'thickness': 10,
    'openLr': 'false',
    'jsonp': 'false'
    }

    try: 
         # Print the full URL to check the request being sent
        print(f"Requesting data for: {point}")
        response = requests.get(url, params=params)
        print(f"Request URL: {response.url}")
        print(f"Response Status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
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
            "road_closure": segment_data.get("roadClosure", "N/A")
        }

    except requests.exceptions.RequestException as e:
        #logging.error(f"Error fetching traffic data for {point}: {e}")
        return None

def fetch_bulk_data(points):
    """Fetch traffic data for all points concurrently."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_traffic_data, points))
    #return results
    return {"traffic_data": [res for res in results if res]}

def main():
    coordinates = read_coordinates_from_csv()
    print(f"First 5 coordinates: {coordinates[:5]}")
    response = fetch_traffic_data(sample_point)
    if response:
        print("Traffic Data Response:")
        print(json.dumps(response, indent=4))
    else:
        print("No traffic data received or an error occurred.")
        
    #print(response)
    #data = fetch_bulk_data(coordinates)

    #print(json.dumps(data, indent=4))
    #with open("traffic_data_bc_sites.json", "w") as file:
    #    json.dump(data, file, indent=4)
        
    #print(json.dumps(data, indent=4))

if __name__ == "__main__":
    main()
