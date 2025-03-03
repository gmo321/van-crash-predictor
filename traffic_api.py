import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

def fetch_traffic_data():
    api_key = ""
    point = "49.2827,-123.1207"
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

    params = {
        "key": api_key,
        "point": point,
        "unit": "KMPH",
        "openLr": "false"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        with open("traffic_data.json", "w") as file:
            json.dump(data, file, indent=4)
        
        return data
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching traffic data: {e}")
        return None

if __name__ == "__main__":
    traffic_data = fetch_traffic_data()
    #print(traffic_data) 
