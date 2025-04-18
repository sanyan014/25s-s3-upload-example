import json
import boto3
import pandas as pd
import psycopg2
from psycopg2 import sql
from scipy.stats import pearsonr

# Database connection variables
DB_HOST = "sleep-data-db.cur84skoqqm0.us-east-1.rds.amazonaws.com"
DB_NAME = "sleepdb"
DB_USER = "postgres"
DB_PASSWORD = "strongpassword!"

s3 = boto3.client("s3")

def lambda_handler(event, context):
    try:
        print("🟡 Start of Lambda Execution")

        # 1. Parse event
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        print(f"📦 Triggered by file: s3://{bucket}/{key}")

        # 2. Try loading from S3
        print("🔄 Attempting to load file from S3...")
        obj = s3.get_object(Bucket=bucket, Key=key)
        print("✅ File fetched from S3")

        # 3. Try parsing JSON
        print("🔍 Parsing JSON...")
        data = json.load(obj["Body"])
        print("✅ JSON parsed")

        # 4. DataFrame conversion
        df = pd.DataFrame(data)
        print("🧾 DataFrame preview:\n", df.head())

        # 5. Compute statistics
        avg_sleep = df["sleep_hours"].mean()
        avg_prod = df["productivity_score"].mean()
        try:
            correlation, _ = pearsonr(df["sleep_hours"], df["productivity_score"])
        except Exception as e:
            print(f"⚠️ Correlation error: {e}")
            correlation = None

        print(f"✅ Stats: avg_sleep={avg_sleep}, avg_prod={avg_prod}, corr={correlation}")

        print("Connecting to the RDS")
        # 6. Connect to RDS PostgreSQL
        conn = psycopg2.connect(
            #host=DB_HOST,
            dbname=DB_NAME
            #user=DB_USER,
            #password=DB_PASSWORD
        )
        cur = conn.cursor()
        print(f"🛠️ Connected to database '{DB_NAME}'")

        # 7. Create table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sleep_stats (
                id SERIAL PRIMARY KEY,
                avg_sleep_hours FLOAT,
                avg_productivity FLOAT,
                correlation FLOAT,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("📐 Table 'sleep_stats' ready.")

        # 8. Insert row into table
        cur.execute("""
            INSERT INTO sleep_stats (avg_sleep_hours, avg_productivity, correlation)
            VALUES (%s, %s, %s)
        """, (avg_sleep, avg_prod, correlation))
        conn.commit()
        print("📥 Inserted data successfully!")

        # 9. Close resources
        cur.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": f"✅ Inserted stats for file: {key}"
        }

    except Exception as e:
        print(f"❌ Lambda Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }   
