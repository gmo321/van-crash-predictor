import requests
import logging
import os
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

weather_data_sample = [
    {
        "name": "Port Coquitlam",
        "latitude": 49.2839,
        "longitude": -122.7933,
        "date": 1742970107,
        "weather": "Clouds",
        "weather_description": "overcast clouds",
        "temp": 11.47,
        "visibility": 10000,
        "clouds": 100,
        "rain": 0,
        "snow": 0
    },
    {
        "name": "Hope",
        "latitude": 49.3797,
        "longitude": -121.4414,
        "date": 1742970107,
        "weather": "Clear",
        "weather_description": "clear sky",
        "temp": 10.96,
        "visibility": 10000,
        "clouds": 0,
        "rain": 0,
        "snow": 0
    },
    {
        "name": "Port Coquitlam",
        "latitude": 49.2617,
        "longitude": -122.7803,
        "date": 1742970107,
        "weather": "Clouds",
        "weather_description": "overcast clouds",
        "temp": 10.86,
        "visibility": 10000,
        "clouds": 100,
        "rain": 0,
        "snow": 0
    }
]

def get_cities():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    
    CITY_LIST_PATH = os.path.join(BASE_DIR, "city_list.csv")
    
    csv_path = os.path.join(os.getcwd(), CITY_LIST_PATH)
    
    df = pd.read_csv(csv_path)
    df.rename(columns={'fullAddress': 'city', 'Y': 'latitude', 'X': 'longitude', }, inplace=True)

    df = df.drop(['addressString', 'score', 'precision', 'faults', 'Notes'], axis=1)
    
    # Reorder dataframe columns
    df = df[['city', 'latitude', 'longitude']]
    
    lats = df['latitude'].values.tolist()
    lons = df['longitude'].values.tolist()
    
    return lats, lons

def fetch_api_data():
    url = 'https://api.openweathermap.org/data/2.5/weather'    
    unit = 'metric'
    
    weather = []
    
    lats, lons = get_cities()

    for lat, lon in zip(lats, lons):

        params = {
            "APIkey": os.getenv("WEATHER_API_KEY"),
            "lat": lat,
            "lon": lon,
            "units": unit
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            response_json = response.json()
            
            lon = response_json.get('coord', {}).get('lon', 0)
            lat = response_json.get('coord', {}).get('lat', 0)
            weather_data = response_json.get('weather', [])
            if weather_data and len(weather_data) > 0:
                weather_main = weather_data[0].get('main', 'No weather available')
                weather_description = weather_data[0].get('description', 'No description available')
            temp = response_json.get('main', {}).get('temp', 'No temperature available')
            visibility = response_json.get('visibility', {})
            clouds = response_json.get('clouds', {}).get('all', 'No cloud % available')
            rain = response_json.get('rain', {}).get('1h', 0)
            snow = response_json.get('snow', {}).get('1h', 0)
            date = response_json.get('dt', {})
            name = response_json.get('name', {})
            
            results = {'name': name, 
                    'latitude': lat, 
                    'longitude': lon, 
                    'date': date, 
                    'weather': weather_main, 
                    'weather_description': weather_description, 
                    'temp': temp,
                    'visibility': visibility, 
                    'clouds': clouds, 
                    'rain': rain, 
                    'snow': snow}
            
            weather.append(results)

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching weather data: {e}")
            continue
            
    with open("weather_data.json", "w") as file:
            json.dump(weather, file, indent=4)
    
    return weather
        
    


def main():
    fetch_api_data()
    
    
if __name__ == '__main__':
    main()
