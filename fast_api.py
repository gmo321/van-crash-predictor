from fastapi import FastAPI
import redis
import json

app = FastAPI()

redis_client = redis.StrictRedis(host="redis-server", port=6379, decode_responses=True)

@app.get("/predict/{name}/{lat}/{lon}")
def get_prediction(name: str, lat: float, lon: float):
    key = f"prediction:{name}:{lat}:{lon}"
    data = redis_client.get(key)

    if data:
        result = json.loads(data)
        return {
            "intersection": f"{name} ({lat}, {lon})",
            "hotspot_risk_level": result.get("hotspot_risk_level"),
            "crash_probability": result.get("crash_probability"),
            "weather": result.get("most_weather"),
            "avg_speed": result.get("avg_speed"),
            "temperature": result.get("avg_temp"),
            "timestamp": result.get("timestamp")
        }

    return {"error": f"No prediction found for {name} ({lat}, {lon})"}
