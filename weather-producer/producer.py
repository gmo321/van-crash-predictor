import requests
from confluent_kafka import Producer
import json
import logging
import time
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
    key = 'Vancouver'
    value = json.dumps(data)
    
    try:
        producer.produce(topic, key=key, value=value, callback=delivery_report)
        producer.flush()
    except Exception as e:
        logging.error(f"Error sending to Kafka: {e}")
        
    
def poll_and_send_data(interval=30):
    while True:
        data = fetch_api_data()
        if data:
            send_to_kafka('weather-data', data)
        time.sleep(interval)

def main():
    logging.info("Starting data polling process.")
    try:
        poll_and_send_data(interval=30)  # Adjust the interval as needed
    except KeyboardInterrupt:
        logging.info("Data polling interrupted.")
    except Exception as e:
        logging.error(f"Error during polling: {e}")
    
if __name__ == '__main__':
    main()
    