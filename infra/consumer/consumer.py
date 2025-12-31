import os
import json
import time
import boto3
from kafka import KafkaConsumer

from dotenv import load_dotenv
load_dotenv()

# ==========================
# Load Environment Variables
# ==========================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET")

# ==========================
# MinIO Client
# ==========================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# ==========================
# Kafka Consumer
# ==========================

consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="bronze-consumers",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("Kafka consumer started. Streaming data to MinIO...")

# ==========================
# Consume & Store
# ==========================

for message in consumer:
    record = message.value

    symbol = record.get("symbol", "UNKNOWN")
    ts = record.get("fetched_at", int(time.time()))

    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json",
    )

    print(f"Saved record for {symbol} → s3://{BUCKET_NAME}/{key}")
