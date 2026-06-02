import json
import boto3
import argparse
import logging

from datetime import datetime
from kafka import TopicPartition, KafkaConsumer

parser = argparse.ArgumentParser()
parser.add_argument("--target_date", required=True)
parser.add_argument("--topic_name", required=True)
parser.add_argument("--broker_name", required=True)
parser.add_argument("--minio_url", required=True)
parser.add_argument("--minio_access_key", required=True)
parser.add_argument("--minio_secret_key", required=True)
parser.add_argument("--bucket_name", required=True)
args = parser.parse_args()

DATE_RUN = args.target_date
TOPIC_NAME = args.topic_name
KAFKA_BROKER = args.broker_name
MINIO_URL = args.minio_url
MINIO_ACCESS = args.minio_access_key
MINIO_SECRET = args.minio_secret_key
BUCKET_NAME = args.bucket_name

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Начинаем с самого начала топика
    enable_auto_commit=False,  # в jupyter никогда не коммитим офсет автоматом!
    consumer_timeout_ms=5000,
)

activities = []

for message in consumer:
    activities.append(message.value)

if activities:
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name='us-east-1'
    )

    dt = datetime.strptime(DATE_RUN, "%Y-%m-%D")
    date_path = dt.strftime("%Y/%m/%d")
    # s3_key = f"raw-data/earthquakes/{context['data_interval_start'].format('YYYY/MM/DD')}/data.json"
    path = f'raw/strava/{date_path}/data.json'
    json_bytes = json.dumps(activities, indent=4, ensure_ascii=False).encode('utf-8')

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=path,
        Body=json_bytes,
        ContentType='application/json',
    )
else:
    logging.warning("No activities found")