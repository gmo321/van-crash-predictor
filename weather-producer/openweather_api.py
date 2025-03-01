import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()

def fetch_api_data():
    LAT = 49.2827
    LON = -123.1207    
    url = 'https://api.openweathermap.org/data/2.5/weather'
    unit = 'metric'

    params = {
        "APIkey": os.getenv("WEATHER_API_KEY"),
        "lat": LAT,
        "lon": LON,
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
        
        results = {'Longitude': lon, 'Latitude': lat, 'weather': weather_main, 'weather_description': weather_description, 'temp': temp,
                        'visibility': visibility, 'clouds': clouds, 'rain': rain, 'snow': snow, 'date': date, 'name': name}
            
        return results
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return None
    

def main():
    fetch_api_data()
    
if __name__ == '__main__':
    main()
    