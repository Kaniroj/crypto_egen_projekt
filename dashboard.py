import streamlit as st
import pandas as pd
import psycopg2
import time

# Hämta data från Postgres
def fetch_data():
    conn = psycopg2.connect(
        dbname="xrp_db",
        user="xrp_user",
        password="xrp_password",
        host="localhost",
        port="5433"
    )
    df = pd.read_sql("SELECT * FROM xrp_data ORDER BY timestamp DESC LIMIT 100", conn)
    conn.close()
    return df

# Streamlit UI
st.title("XRP Realtidsdashboard")
chart_placeholder = st.empty()
table_placeholder = st.empty()

while True:
    df = fetch_data()
    chart_placeholder.line_chart(df[['price_usd']].set_index(df['timestamp']))
    table_placeholder.table(df[['timestamp', 'price_usd', 'volume_24h']].head(10))
    time.sleep(10)  # Uppdatera var 10:e sekund