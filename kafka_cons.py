import json
from kafka import KafkaConsumer
import boto3
from datetime import datetime

# AWS S3 Configuration
S3_BUCKET = 'harsh-adt-bucket'
S3_FOLDER = 'sql'  # Optional: to organize your files in a specific folder within the bucket

# Initialize S3 client
s3_client = boto3.client('s3')

# Kafka Configuration
KAFKA_TOPIC = 'demo_testing2'
KAFKA_BOOTSTRAP_SERVERS = ['18.191.110.113:9092']

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def upload_to_s3(data, filename):
    try:
        s3_key = f"{S3_FOLDER}/{filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data).encode('utf-8')
        )
        print(f"Successfully uploaded {filename} to S3 with key {s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Consume messages from Kafka and upload to S3
for message in consumer:
    data = message.value
    if data['type'] == 'data':
        # Create a unique filename based on current timestamp for data
        filename = f"data_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json"
        upload_to_s3(data['value'], filename)
    elif data['type'] == 'schema':
        # Extract the table name from the schema
        table_name = data['value']['Columns'][0]['TABLE_NAME']
        # Create filename based on the table name
        filename = f"schema_{table_name}.json"
        upload_to_s3(data['value'], filename)
    else:
        print(f"Unknown message type received: {data['type']}")

