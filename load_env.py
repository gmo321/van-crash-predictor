from dotenv import load_dotenv
import os

def load_environment_variables():
    # Load .env file into the environment
    load_dotenv()

    # Access variables
    jupyter_token = os.getenv("JUPYTER_TOKEN")
    weather_api_key = os.getenv("WEATHER_API_KEY")
    traffic_api_key = os.getenv("TRAFFIC_API_KEY")
    kafka_url = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

    return {
        "JUPYTER_TOKEN": jupyter_token,
        "WEATHER_API_KEY": weather_api_key,
        "TRAFFIC_API_KEY": traffic_api_key,
        "KAFKA_BROKER_URL": kafka_url,
    }