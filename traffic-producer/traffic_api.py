import requests
import os
import json
import logging
import concurrent.futures
from dotenv import load_dotenv

load_dotenv()

points = [
    "49.2827,-123.1207",  # Downtown Vancouver
    "49.2600,-123.1135",  # East Vancouver
    "49.2800,-123.1000",  # West End
    "49.2767,-123.1121",  # Near Stanley Park
    "49.2690,-123.1122",  # Near Vancouver International Airport (YVR)
    "49.2895,-123.1150"   # False Creek area    
]


api_key = os.getenv("TRAFFIC_API_KEY")
url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"


def fetch_traffic_data(point):
    """
    Fetch traffic data for a given point.
    Args:
        point (str): The geographical point for which to fetch traffic data.
    Returns:
        dict: The traffic data in JSON format.
    Raises:
        requests.exceptions.RequestException: If the request to the traffic API fails.
    """
    
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
            "road_closure": segment_data.get("roadClosure", "N/A"),
            "date": response.headers.get("Date", "N/A")
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching traffic data for {point}: {e}")
        return None

def fetch_bulk_data():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(fetch_traffic_data, points
))
    return {"traffic_data": [res for res in results if res]}

def main():
    data = fetch_bulk_data()
    print(data)
    with open("traffic_data.json", "w") as file:
        json.dump(data, file, indent=4)

if __name__ == "__main__":
    main()


def fetch_bulk_data():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(fetch_traffic_data, points)
    return {"traffic_data": list(results)}

bulk_data = fetch_bulk_data()
with open("traffic_data.json", "w") as file:
    json.dump(bulk_data, file, indent=4)
