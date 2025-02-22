from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink
from constants import (
    POSTGRES_DBNAME,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

# Extrahera och transformera data
def extract_coin_data(message):
    latest_quote = message["quote"]["USD"]
    price_usd = latest_quote["price"]
    price_sek = update_price_in_currency(price_usd, "SEK")
    price_dkk = update_price_in_currency(price_usd, "DKK")
    price_nok = update_price_in_currency(price_usd, "NOK")
    price_isk = update_price_in_currency(price_usd, "ISK")
    price_eur = update_price_in_currency(price_usd, "EUR")
    change_24h = latest_quote["percent_change_24h"]

    return {
        "coin": message["name"],
        "price_sek": price_sek,
        "price_dkk": price_dkk,
        "price_nok": price_nok,
        "price_isk": price_isk,
        "price_eur": price_eur,
        "volume": latest_quote["volume_24h"],
        "updated": message["last_updated"],
        "percent_change_24h": change_24h
    }

# Hårdkodade växelkurser
def get_exchange_rate_hardcoded(target_currency):
    exchange_rates = {
        "SEK": 10.7,
        "DKK": 7.1,
        "NOK": 11.2,
        "ISK": 140.0,
        "EUR": 0.95
    }
    return exchange_rates.get(target_currency, None)

def update_price_in_currency(price_in_usd, target_currency):
    exchange_rate = get_exchange_rate_hardcoded(target_currency)
    if exchange_rate:
        return round(price_in_usd * exchange_rate, 3)
    else:
        print(f"Cannot update price for {target_currency}.")
        return None

# Skapa Postgres-sink
def create_postgres_sink():
    return PostgreSQLSink(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DBNAME,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        table_name="xrp",
        schema_auto_update=True,
    )

def main():
    app = Application(
        broker_address="localhost:9092",
        consumer_group="coin_group",
        auto_offset_reset="earliest",
    )
    coins_topic = app.topic(name="coins", value_deserializer="json")

    sdf = app.dataframe(topic=coins_topic)
    sdf = sdf.apply(extract_coin_data)

    # Skriv ut data för debugging
    sdf.update(lambda coin_data: print(
        f"Coin Data:\n"
        f"Coin: {coin_data['coin']}\n"
        f"Price in SEK: {coin_data['price_sek']:.3f}\n"
        f"Price in DKK: {coin_data['price_dkk']:.3f}\n"
        f"Price in NOK: {coin_data['price_nok']:.3f}\n"
        f"Price in ISK: {coin_data['price_isk']:.3f}\n"
        f"Price in EUR: {coin_data['price_eur']:.3f}\n"
        f"Volume: {coin_data['volume']:.3f}\n"
        f"Updated: {coin_data['updated']}\n"
        f"Percent change 24h: {coin_data['percent_change_24h']}"
    ))

    # Spara till Postgres
    postgres_sink = create_postgres_sink()
    sdf.sink(postgres_sink)

    app.run()

if __name__ == "__main__":
    main()