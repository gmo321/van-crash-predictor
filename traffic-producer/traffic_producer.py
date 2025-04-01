import requests
from confluent_kafka import Producer
import json
import logging
import time
from traffic_api import fetch_traffic_data
from azure_maps import fetch_bulk_data
from azure_maps import read_coordinates_from_csv

logging.basicConfig(level=logging.DEBUG)

# Kafka configuration
KAFKA_TOPIC = "traffic-data"
KAFKA_SERVER = "kafka:9092"

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    
def send_to_kafka(producer, topic, data):
    # Sends message to topic
    key = 'Vancouver'
    value = json.dumps(data)
    
    try:
        producer.produce(topic, key=key, value=value, callback=delivery_report)
        producer.flush()
    except Exception as e:
        logging.error(f"Error sending to Kafka: {e}")
        
        
def create_producer():
    config = {
        'bootstrap.servers': KAFKA_SERVER
    }
    # Return Producer instance
    return Producer(config)
        
    
def poll_and_send_data(producer, interval=30, csv_path="bc_sites2.csv"):
    points = read_coordinates_from_csv(csv_path)
    #point = "49.2827,-123.1207"
    
    while True:
        data = fetch_bulk_data(points)
        if data:
            for point in data['traffic_data']:  
                send_to_kafka(producer, 'traffic-data', point)

        time.sleep(interval)

def main():
    logging.info("Starting data polling process.")
    try:
        producer = create_producer()
        poll_and_send_data(producer, interval=30, csv_path="bc_sites2.csv")  
    except KeyboardInterrupt:
        logging.info("Data polling interrupted.")
    except Exception as e:
        logging.error(f"Error during polling: {e}")
    finally:
        producer.flush()
    
if __name__ == '__main__':
    main()
    