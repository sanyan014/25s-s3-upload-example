import streamlit as st
import os
import json
import boto3
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from io import StringIO

# Load environment variables
load_dotenv()

# AWS & RDS CONFIG
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
RAW_BUCKET = os.getenv("S3_BUCKET_NAME", "ds4300-sleep-raw")
RESULT_BUCKET = "ds4300-sleep-results"
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

# Streamlit setup
st.set_page_config(page_title="Sleep Pipeline", layout="wide")
st.title("😴 Sleep Data Pipeline")

# === Upload multiple files ===
uploaded_files = st.file_uploader("📁 Upload your JSON files", type="json", accept_multiple_files=True)
if uploaded_files:
    for file in uploaded_files:
        try:
            s3.upload_fileobj(file, RAW_BUCKET, f"uploads/{file.name}")
            st.success(f"✅ Uploaded: {file.name}")
        except Exception as e:
            st.error(f"❌ Failed to upload {file.name}: {e}")

# === Download & store to RDS from results bucket ===
if st.button("⬇️ Sync Results to RDS"):
    try:
        response = s3.list_objects_v2(Bucket=RESULT_BUCKET, Prefix="results/")
        if "Contents" not in response:
            st.warning("⚠️ No result files found.")
        else:
            # Connect to your RDS PostgreSQL instance
            conn = psycopg2.connect(
                host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            cur = conn.cursor()

            # Ensure the table exists with a unique constraint on filename
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sleep_stats (
                    id SERIAL PRIMARY KEY,
                    filename TEXT UNIQUE,
                    avg_sleep_hours FLOAT,
                    avg_productivity FLOAT,
                    correlation FLOAT,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

            inserted = 0
            for obj in response["Contents"]:
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue

                obj_data = s3.get_object(Bucket=RESULT_BUCKET, Key=key)
                stats = json.load(obj_data["Body"])

                cur.execute("""
                    INSERT INTO sleep_stats (filename, avg_sleep_hours, avg_productivity, correlation)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (filename) DO NOTHING
                    RETURNING id
                """, (
                    stats["filename"],
                    float(stats["avg_sleep_hours"]),
                    float(stats["avg_productivity"]),
                    float(stats["correlation"]) if stats["correlation"] is not None else None
                ))

                if cur.fetchone():
                    inserted += 1

            conn.commit()
            cur.close()
            conn.close()
            st.success(f"✅ Synced {inserted} new result(s) to RDS")
    except Exception as e:
        st.error(f"❌ Sync failed: {e}")


# === Load & display RDS data ===
def load_all_data():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        df = pd.read_sql("""
            SELECT filename,
                   ROUND(avg_sleep_hours::numeric, 2) AS avg_sleep,
                   ROUND(avg_productivity::numeric, 2) AS avg_productivity,
                   ROUND(correlation::numeric, 2) AS correlation,
                   timestamp
            FROM sleep_stats
            ORDER BY timestamp DESC
        """, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Could not load RDS data: {e}")
        return pd.DataFrame()

# Show table
df = load_all_data()
st.markdown("### 📊 All Sleep Analysis Results")
if not df.empty:
    st.dataframe(df, use_container_width=True)

    # Download CSV
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download All as CSV", data=csv, file_name="sleep_stats.csv", mime="text/csv")
else:
    st.warning("⚠️ No data available in RDS.")
