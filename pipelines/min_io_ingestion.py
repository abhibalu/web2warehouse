import boto3
from botocore.client import Config
import os

# Step 1: Set up the MinIO client using boto3
s3 = boto3.client(
    's3',
    endpoint_url='http://127.0.0.1:9000',  # MinIO endpoint
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'  # MinIO doesn’t enforce regions, but boto3 expects one
)

# Step 2: Create a bucket
bucket_name = 'staging'
try:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created.")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"Bucket '{bucket_name}' already exists.")

# Step 3: Upload a file
file_path = 'scraped_data/properties.ndjson'
object_name = os.path.basename(file_path)

s3.upload_file(file_path, bucket_name, object_name)
print(f"Uploaded '{file_path}' to bucket '{bucket_name}' as '{object_name}'.")
