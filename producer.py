from kafka import KafkaProducer
import json
import requests
import time

# Kafka-konfiguration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Hämta XRP-data från CoinGecko
def fetch_xrp_data():
    url = "https://api.coingecko.com/api/v3/coins/ripple"
    response = requests.get(url)
    data = response.json()
    return {
        "name": data["name"],
        "quote": {
            "USD": {
                "price": data["market_data"]["current_price"]["usd"],
                "volume_24h": data["market_data"]["total_volume"]["usd"],
                "percent_change_24h": data["market_data"]["price_change_percentage_24h"]
            }
        },
        "last_updated": data["last_updated"]
    }

# Skicka data till Kafka
def send_to_kafka():
    while True:
        xrp_data = fetch_xrp_data()
        producer.send('coins', xrp_data)  # Matchar topic i consumer.py
        print(f"Skickat: {xrp_data}")
        time.sleep(60)  # Uppdatera var 60:e sekund

if __name__ == "__main__":
    send_to_kafka()