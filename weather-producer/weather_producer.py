import requests
from confluent_kafka import Producer
import json
import logging
import time
from openweather_api import get_cities
from openweather_api import fetch_api_data

logging.basicConfig(level=logging.DEBUG)

    
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
def send_to_kafka(topic, data):
    config = {
        'bootstrap.servers': 'kafka:9092'
    }
    
    # Create Producer instance
    producer = Producer(config)
    
    # Sends message to topic
    #key = 'weather'

    if isinstance(data, list):
        for record in data:
            if isinstance(record, dict):
                municipality = record.get('name', 'unknown')
            #value = json.dumps(record)
                producer.produce(topic, key=municipality, value=json.dumps(record), callback=delivery_report)
            else:
                logging.warning(f"Received non-dict record: {record}")
    elif isinstance(data, dict):
        municipality = data.get('name', 'unknown')  # Fallback in case 'name' is missing
        #value = json.dumps(data)
        producer.produce(topic, key=municipality, value=json.dumps(data), callback=delivery_report)
        
    # Flush after sending all records
    producer.flush()

        
    
def poll_and_send_data(interval=30):
    while True:
        get_cities()
        data = fetch_api_data()
        if not data:
            logging.warning("No data received from the API.")
            continue
        
        send_to_kafka('weather-data', data)
        time.sleep(interval)

def main():
    logging.info("Starting data polling process.")
    try:
        poll_and_send_data(interval=30)  
    except KeyboardInterrupt:
        logging.info("Data polling interrupted.")
    except Exception as e:
        logging.error(f"Error during polling: {e}")
    
if __name__ == '__main__':
    main()
    