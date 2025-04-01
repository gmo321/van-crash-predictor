import requests
import logging
import csv
import json
import concurrent.futures

api_key = ""
url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

def read_coordinates_from_csv(file_path):
    """Read longitude and latitude from the CSV file."""
    coordinates = []
    with open(file_path, mode='r') as file:
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
        'openLr': False,
        'jsonp': False
    }
    
    try: 
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        segment_data = data.get("flowSegmentData", {})
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
        logging.error(f"Error fetching traffic data for {point}: {e}")
        return None

def fetch_bulk_data(points):
    """Fetch traffic data for all points concurrently."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(fetch_traffic_data, points))
    return {"traffic_data": [res for res in results if res]}

def main():
    coordinates = read_coordinates_from_csv("bc_sites2.csv")
    data = fetch_bulk_data(coordinates)
    print(json.dumps(data, indent=4))
    with open("traffic_data_bc_sites.json", "w") as file:
        json.dump(data, file, indent=4)

if __name__ == "__main__":
    main()
