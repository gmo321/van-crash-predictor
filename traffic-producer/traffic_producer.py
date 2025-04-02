import requests
from confluent_kafka import Producer
import json
import logging
import time
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
    
    try:
        for point in data.get("traffic_data", []):
            value = json.dumps(point)
            #print(value)
            producer.produce(topic, key=key, value=value, callback=delivery_report)
            
    except Exception as e:
        logging.error(f"Error sending to Kafka: {e}")
        
    producer.poll(0)
    producer.flush()
    logging.info("Data sent successfully!")
        
        
def create_producer():
    config = {
        'bootstrap.servers': KAFKA_SERVER,
        'message.max.bytes': 200000000
    }
    # Return Producer instance
    return Producer(config)
        
    
def poll_and_send_data(producer, interval=3600):

    points = read_coordinates_from_csv()
    #points = ["49.2827,-123.1207"]
    #for point in points:
        
    while True:
        try:
            #data = fetch_traffic_data(point)
            data = fetch_bulk_data(points)
            
            #print(json.dumps(data, indent=4))
            if not data or 'traffic_data' not in data:
                logging.warning("No data received from the API.")
                continue
    
            send_to_kafka(producer, KAFKA_TOPIC, data)

        except Exception as e:
            logging.error(f"Error during polling: {e}")
            logging.debug("Traceback", exc_info=True)
            
        time.sleep(interval)

def main():
    logging.info("Starting data polling process.")
    try:
        producer = create_producer()
        poll_and_send_data(producer, interval=3600)  
    except KeyboardInterrupt:
        logging.info("Data polling interrupted.")
    except Exception as e:
        logging.error(f"Error during polling: {e}")
    
if __name__ == '__main__':
    main()
    