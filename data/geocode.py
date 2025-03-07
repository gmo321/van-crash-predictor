import requests
import logging
import os

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
        
def main():
    address = "Vancouver, BC"
    geocode_data(address)
    
if __name__ == '__main__':
    main()