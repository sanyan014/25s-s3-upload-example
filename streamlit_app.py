import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px

# Load environment variables or set directly
DB_HOST = os.getenv("DB_HOST", "sleep-prod-db.cur84skoqqm0.us-east-1.rds.amazonaws.com")
DB_NAME = os.getenv("DB_NAME", "sleepdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "strongpassword!")  # Use secrets in production

def get_data():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        query = "SELECT * FROM sleep_stats ORDER BY timestamp DESC LIMIT 50"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return pd.DataFrame()

# Streamlit layout
st.set_page_config(page_title="Sleep Stats Dashboard", layout="wide")
st.title("📈 Sleep Analytics Dashboard")
st.markdown("Monitor sleep quality, productivity, and correlation over time.")

df = get_data()

if df.empty:
    st.warning("No data available yet.")
else:
    # Overview metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Avg Sleep Hours", round(df['avg_sleep_hours'].mean(), 2))
    col2.metric("Avg Productivity", round(df['avg_productivity'].mean(), 2))
    col3.metric("Correlation", round(df['correlation'].mean(), 2))

    # Plotting
    st.subheader("🕒 Sleep & Productivity Over Time")
    fig = px.line(df, x="timestamp", y=["avg_sleep_hours", "avg_productivity"], markers=True)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("📌 Correlation Trend")
    fig_corr = px.line(df, x="timestamp", y="correlation", markers=True)
    st.plotly_chart(fig_corr, use_container_width=True)

    # Table
    st.subheader("📄 Raw Data Table")
    st.dataframe(df)

