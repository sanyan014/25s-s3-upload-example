import json
import boto3
import psycopg2

# === CONFIG ===
RESULT_BUCKET = "ds4300-sleep-results"
DB_HOST = "sleep-data-db.cur84skoqqm0.us-east-1.rds.amazonaws.com"
DB_NAME = "sleepdb"
DB_USER = "postgres"
DB_PASSWORD = "strongpassword!"

# Allow toggling local execution
run_local = True

# === S3 Client ===
s3 = boto3.client("s3")

def analyze_and_store(bucket, key):
    try:
        print(f"📦 Fetching results file: s3://{bucket}/{key}")

        # 1. Load precomputed result from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        summary = json.load(obj["Body"])
        print("✅ Loaded summary data from result file")

        # 2. Extract clean filename from the result key (e.g., "results/gan_results.json")
        filename = key.split("/")[-1]  # force it to be the result filename itself
        avg_sleep = float(summary.get("avg_sleep_hours", 0))
        avg_prod = float(summary.get("avg_productivity", 0))
        corr = summary.get("correlation", None)
        if corr is not None:
            corr = float(corr)

        # 3. Insert into DB (optional)
        if run_local:
            print("💾 Inserting into PostgreSQL...")
            conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            cur = conn.cursor()
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

            cur.execute("""
                INSERT INTO sleep_stats (filename, avg_sleep_hours, avg_productivity, correlation)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (filename) DO NOTHING
            """, (filename, avg_sleep, avg_prod, corr))
            conn.commit()

            cur.close()
            conn.close()
            print("✅ Inserted into DB")

        return {
            "statusCode": 200,
            "body": f"✅ Results stored for {filename}"
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }

# === Lambda Entrypoint ===
def lambda_handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    return analyze_and_store(bucket, key)

# === Local Testing ===
if __name__ == "__main__":
    test_bucket = "ds4300-sleep-results"
    test_key = "results/gan_results.json"
    analyze_and_store(test_bucket, test_key)
