import streamlit as st
import pandas as pd
import psycopg2
import time
from constants import (
    POSTGRES_DBNAME,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

# Hämta data från Postgres
def fetch_data():
    conn = psycopg2.connect(
        dbname=POSTGRES_DBNAME,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    df = pd.read_sql("SELECT * FROM xrp ORDER BY updated DESC LIMIT 100", conn)
    conn.close()
    return df

# Streamlit-dashboard
st.title("XRP Realtidsdashboard")

# Val av valuta
currency = st.selectbox("Välj valuta", ["SEK", "DKK", "NOK", "ISK", "EUR"])

# Platshållare
chart_placeholder = st.empty()
table_placeholder = st.empty()

# Realtidsuppdatering
while True:
    df = fetch_data()
    df['updated'] = pd.to_datetime(df['updated'])  # Konvertera till datetime

    # Välj pris baserat på valuta
    price_col = f"price_{currency.lower()}"
    chart_placeholder.line_chart(df.set_index('updated')[[price_col]])

    # Visa tabell med relevant data
    table_data = df[['updated', price_col, 'volume', 'percent_change_24h']].head(10)
    table_placeholder.table(table_data)

    time.sleep(10)  # Uppdatera var 10:e sekund