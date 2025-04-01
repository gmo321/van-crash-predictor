import requests
from confluent_kafka import Producer
import json
import logging
import time
import os
from openweather_api import get_cities
from openweather_api import fetch_api_data

logging.basicConfig(level=logging.DEBUG)

# Kafka configuration
KAFKA_TOPIC = "weather-data"
KAFKA_SERVER = "kafka:9092"


def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
def send_to_kafka(producer, topic, data):

    if isinstance(data, list):
        for record in data:
            if isinstance(record, dict):
                municipality = record.get('name', 'unknown')
                producer.produce(topic, key=municipality, value=json.dumps(record), callback=delivery_report)
            else:
                logging.warning(f"Received non-dict record: {record}")
    elif isinstance(data, dict):
        municipality = data.get('name', 'unknown')  
        #value = json.dumps(data)
        producer.produce(topic, key=municipality, value=json.dumps(data), callback=delivery_report)
        
    # Flush after sending all records
    producer.poll(0)
    producer.flush()
    logging.info("Data sent successfully!")
    

def create_producer():
    config = {
        'bootstrap.servers': KAFKA_SERVER
    }
    # Return Producer instance
    return Producer(config)
        
    
def poll_and_send_data(producer, interval=30):
    while True:
        try:
            data = fetch_api_data()
            
            #logging.debug(f"Fetched data: {data}")  # Log API data
            
            if not data:
                logging.warning("No data received from the API.")
                continue
            
            send_to_kafka(producer, KAFKA_TOPIC, data)
            
        except Exception as e:
            logging.error(f"Error during polling: {e}")
        
        time.sleep(interval)

def main():
    logging.info("Starting data polling process.")
    try:
        producer = create_producer()
        poll_and_send_data(producer, interval=30)  
    except KeyboardInterrupt:
        logging.info("Data polling interrupted.")
    except Exception as e:
        logging.error(f"Error during polling: {e}")
    
if __name__ == '__main__':
    main()
    