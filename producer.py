from kafka import KafkaProducer
import json
import requests
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_xrp_data():
    url = "https://api.coingecko.com/api/v3/coins/ripple"
    response = requests.get(url)
    data = response.json()
    return {
        "timestamp": data["last_updated"],
        "price_usd": data["market_data"]["current_price"]["usd"],
        "volume_24h": data["market_data"]["total_volume"]["usd"]
    }

def send_to_kafka():
    while True:
        xrp_data = fetch_xrp_data()
        producer.send('xrp_topic', xrp_data)
        print(f"Skickat: {xrp_data}")
        time.sleep(60)  # Uppdatera var 60:e sekund

if __name__ == "__main__":
    send_to_kafka()