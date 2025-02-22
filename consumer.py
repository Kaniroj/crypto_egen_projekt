from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import Error

# Postgres-konfiguration
def init_db():
    conn = psycopg2.connect(
        dbname="xrp_db",
        user="xrp_user",
        password="xrp_password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS xrp_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            price_usd FLOAT,
            volume_24h FLOAT
        );
    """)
    conn.commit()
    return conn

# Kafka-konsument
consumer = KafkaConsumer(
    'xrp_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Spara till Postgres
def save_to_postgres(conn, data):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO xrp_data (timestamp, price_usd, volume_24h)
        VALUES (%s, %s, %s);
    """, (data['timestamp'], data['price_usd'], data['volume_24h']))
    conn.commit()

if __name__ == "__main__":
    conn = init_db()
    for message in consumer:
        xrp_data = message.value
        save_to_postgres(conn, xrp_data)
        print(f"Sparat: {xrp_data}")