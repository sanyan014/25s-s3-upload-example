# app.py
import streamlit as st
import psycopg2
import pandas as pd

# DB credentials
DB_HOST = "sleep-data-db.cur84skoqqm0.us-east-1.rds.amazonaws.com"
DB_NAME = "sleepdb"
DB_USER = "postgres"
DB_PASSWORD = "strongpassword!"

st.title("Sleep Analysis Dashboard")

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM sleep_stats ORDER BY timestamp DESC LIMIT 20")
    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["ID", "Avg Sleep", "Avg Productivity", "Correlation", "Timestamp"])
    st.dataframe(df)

    cur.close()
    conn.close()
except Exception as e:
    st.error(f"Failed to load data: {e}")
