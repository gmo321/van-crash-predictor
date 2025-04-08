import requests
import logging
import os
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def fetch_api_geocode():  
    url = 'http://api.openweathermap.org/geo/1.0/direct'

    municipalities_list = ['coquitlam', 'hope', 'port coquitlam', 'revelstoke', 'chilliwack', 'creston', 'coombs', 'sparwood', 'halfmoon bay', 'keremeos', 'port moody', 
                           'lantzville', 'terrace', 'grand forks', 'central saanich', 'kelowna', 'mill bay', 'port alberni', 'saanich', 'cranbrook', 'parksville', 'langley', 
                            'powell river', 'duncan', 'squamish', 'sechelt', 'houston', 'white rock', 'maple ridge', 'salmo', 'merritt', 'north saanich', 'dawson creek', 
                            'prince george', 'black creek', 'castlegar', 'williams lake', 'esquimalt', 'surrey', 'coldstream', 'fort st john', 'new westminster', 'mission', 
                            'nanaimo', 'salt spring island', 'lake country', 'summerland', 'west vancouver', 'burnaby', 'kamloops', 'view royal', 'lee creek', 'ubc', 'victoria', 
                            'montrose', 'enderby', 'golden', 'west kelowna', 'abbotsford', 'chemainus', 'qualicum beach', 'sooke', 'agassiz', 'lake cowichan', 'trail', 'pemberton', 
                            'vernon', 'cobble hill', 'ladysmith', 'nanoose bay', 'penticton', 'campbell river', 'colwood', 'quesnel', 'pitt meadows', 'courtenay', 'delta', 'oak bay', 
                            'shawnigan lake', 'richmond', 'whistler', 'elkford', 'gibsons', 'comox', 'salmon arm', 'vancouver', 'langford', 'north vancouver', 'oliver', 'peachland', 
                            'cowichan bay', 'fernie', 'tappen', 'chetwynd', 'deroche', 'sidney', 'nelson', 'christina lake', 'mclure', 'blind bay', 'okanagan falls', 'armstrong', 
                            'vanderhoof', 'roberts creek', 'clearwater', 'metchosin', 'port hardy', 'wasa', 'kaleden', 'malahat', 'prince rupert', 'osoyoos', 'lake errock', 'sicamous',
                            'hatzic', 'sirdar', 'princeton', 'sorrento', 'smithers', 'spallumcheen', 'kersley', 'charlie lake', 'fraser lake', 'ymir', 'fruitvale', 'kimberley', 
                            'invermere', 'lions bay', 'wycliffe', 'aspen grove', 'bridesville', 'falkland', 'woodpecker', 'thornhill', 'mackenzie', 'radium hot springs', 'groundbirch',
                            'fairmont hot springs', 'erie', 'union bay', 'dewdney', 'popkum']

    # Add ", BC" to each municipality
    #municipalities_with_bc = [f"{municipality}, BC, CA" for municipality in municipalities_list]

    # Convert the list to a DataFrame
    #df = pd.DataFrame(municipalities_with_bc, columns=["addressString"])
    
    #df.to_csv('municipalities_bc.csv', index=False)

    # Show the resulting DataFrame
    #print(df)
                           
    city_list = []
    
    for municipalities in municipalities_list:
        city_name = f"{municipalities}, BC, CA".lower()
        
        params = {
            "appid": os.getenv("WEATHER_API_KEY"),
            "q": municipalities,
            "limit": 5
        }

        try:
            response = requests.get(url, params)
            response.raise_for_status()
            response_json = response.json()
            
            # Dictionary for manually added cities
            manual_coords = {
                "lee creek, BC, CA": {"lat": 50.8987, "lon": -119.3985},
                "ubc, BC, CA": {"lat": 49.2606, "lon": -123.2460},
                "montrose, BC, CA": {"lat": 49.0954, "lon": -117.6023},
                "vernon, BC, CA": {"lat": 50.2670, "lon": -119.2720},
                "mclure, BC, CA": {"lat": 50.9057, "lon": -120.1058},
                "sirdar, BC, CA": {"lat": 49.3667, "lon": -116.5833},
                "princeton, BC, CA": {"lat": 49.4585, "lon": -120.5108},
                "kersley, BC, CA": {"lat": 52.8916, "lon": -122.4544},
                "wycliffe, BC, CA": {"lat": 49.6375, "lon": -115.7469},
                "bridesville, BC, CA": {"lat": 49.0514, "lon": -119.3824},
                "thornhill, BC, CA": {"lat": 54.4831, "lon": -128.5900},
                "groundbirch, BC, CA": {"lat": 55.6333, "lon": -121.4167},
                "erie, BC, CA": {"lat": 49.1167, "lon": -117.7000},
                # Add more cities if necessary
            }
            
            # Filter results based on country and state
            filtered_results = [item for item in response_json if item['country'] == 'CA' and item['state'] == 'British Columbia']
            
            # Example city to look up
            city_name = "lee creek, BC, CA".lower()
            
            # Check if city is in the manual list
            if city_name.lower() in manual_coords:
                lat = manual_coords[city_name.lower()]["lat"]
                lon = manual_coords[city_name.lower()]["lon"]
                name = city_name
            else:
                if filtered_results: 
                    name = filtered_results[0].get('name', 'N/A')
                    lat = filtered_results[0].get('lat', 0)
                    lon = filtered_results[0].get('lon', 0)
                    country = filtered_results[0].get('country', 'N/A')
                    state = filtered_results[0].get('state', 'N/A')
                    
                    results = {'city': name, 'latitude': lat, 'longitude': lon, 'country': country, 'state':state}  
                
                    city_list.append(results)
                else:
                    logging.warning(f"No results found for {municipalities}")
                
                 
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching location data for {municipalities}: {e}")
            return None
            
    #with open("city_data.json", "w") as file:
    #            json.dump(city_list, file, indent=4)
    
                

def main():
    fetch_api_geocode()
    
if __name__ == '__main__':
    main()
    