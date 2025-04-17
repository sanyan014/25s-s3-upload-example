import json
import os
import boto3
import pandas as pd
import psycopg2
from io import BytesIO
from scipy.stats import pearsonr

# Get env variables
DB_HOST = "sleep-prod-db.cur84skoqqm0.us-east-1.rds.amazonaws.com"
DB_NAME = "sleepdb"
DB_USER = "postgres"
DB_PASSWORD = "strongpassword!"

s3 = boto3.client("s3")

def lambda_handler(event, context):
    try:
        # 1. Parse event
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        print(f"Triggered by file: s3://{bucket}/{key}")

        # 2. Read JSON file from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.load(obj["Body"])

        # 3. Convert to DataFrame
        df = pd.DataFrame(data)
        print("Loaded data:", df.head())

        # 4. Compute statistics
        avg_sleep = df["sleep_hours"].mean()
        avg_prod = df["productivity_score"].mean()

        # Handle correlation safely
        try:
            correlation, _ = pearsonr(df["sleep_hours"], df["productivity_score"])
        except Exception:
            correlation = None

        print(f"avg_sleep={avg_sleep}, avg_prod={avg_prod}, corr={correlation}")

        # 5. Insert into RDS
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        # Create table if not exists
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

        # Insert data
        cur.execute("""
            INSERT INTO sleep_stats (avg_sleep_hours, avg_productivity, correlation)
            VALUES (%s, %s, %s)
        """, (avg_sleep, avg_prod, correlation))

        conn.commit()
        cur.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": f"Inserted stats for {key}"
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }
