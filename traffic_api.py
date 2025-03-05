import requests
import os
import json
import concurrent.futures

points = [
    "49.2827,-123.1207",  # Downtown Vancouver
    "49.2600,-123.1135",  # East Vancouver
    "49.2800,-123.1000",  # West End
    "49.2767,-123.1121",  # Near Stanley Park
    "49.2690,-123.1122",  # Near Vancouver International Airport (YVR)
    "49.2895,-123.1150"   # False Creek area    
]


api_key = ""
url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"


def fetch_traffic_data(point):
    """
    Fetch traffic data for a given point.
    Args:
        point (str): The geographical point for which to fetch traffic data.
    Returns:
        dict: The traffic data in JSON format.
    """
    
    params = {
        'key': api_key,
        'point': point,
        'unit': 'KMPH',
        'thickness': 10,
        'openLr': False,
        'jsonp': False
    }
    response = requests.get(url, params=params)
    return response.json()

def fetch_bulk_data():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(fetch_traffic_data, points)
    return {"traffic_data": list(results)} 

bulk_data = fetch_bulk_data()
with open("traffic_data.json", "w") as file:
    json.dump(bulk_data, file, indent=4)
