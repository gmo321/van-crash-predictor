from dotenv import load_dotenv
import os

def load_environment_variables():
    # Load .env file into the environment
    load_dotenv()

    # Access variables
    jupyter_token = os.getenv("JUPYTER_TOKEN")
    api_key = os.getenv("WEATHER_API_KEY")
    kafka_url = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

    # Optional: Print to verify (remove in production)
    print(f"Jupyter Token: {jupyter_token}")
    print(f"API Key: {api_key}")
    print(f"Kafka Broker URL: {kafka_url}")

    return {
        "JUPYTER_TOKEN": jupyter_token,
        "API_KEY": api_key,
        "KAFKA_BROKER_URL": kafka_url,
    }