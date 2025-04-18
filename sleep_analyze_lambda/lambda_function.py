import json
import boto3
import pandas as pd
from scipy.stats import pearsonr

s3 = boto3.client("s3")
RESULT_BUCKET = "ds4300-sleep-results"

def lambda_handler(event, context):
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.load(obj["Body"])
        df = pd.DataFrame(data)

        avg_sleep = df["sleep_hours"].mean()
        avg_prod = df["productivity_score"].mean()
        try:
            corr, _ = pearsonr(df["sleep_hours"], df["productivity_score"])
        except Exception:
            corr = None

        original_name = key.split("/")[-1].replace("_clean.json", "").replace(".json", "")
        result_key = f"results/{original_name}_results.json"

        # ✅ Check if results file already exists
        existing = s3.list_objects_v2(Bucket=RESULT_BUCKET, Prefix=result_key)
        if "Contents" in existing:
            print(f"⚠️ Skipping: {result_key} already exists in S3.")
            return {
                "statusCode": 200,
                "body": f"⚠️ Results file already exists: {result_key}"
            }

        summary = {
            "filename": key,
            "avg_sleep_hours": avg_sleep,
            "avg_productivity": avg_prod,
            "correlation": corr
        }

        s3.put_object(
            Bucket=RESULT_BUCKET,
            Key=result_key,
            Body=json.dumps(summary),
            ContentType="application/json"
        )

        print(f"✅ Summary saved to {RESULT_BUCKET}/{result_key}")

        return {
            "statusCode": 200,
            "body": f"✅ Summary uploaded as {result_key}"
        }

    except Exception as e:
        print(f"❌ Lambda Error: {e}")
        return {
            "statusCode": 500,
            "body": f"Failed: {str(e)}"
        }
