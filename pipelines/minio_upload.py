# minio_upload.py

import boto3
from botocore.client import Config
import os


def create_minio_client(
    endpoint_url: str, access_key_id: str, secret_access_key: str
) -> "boto3.client":
    """
    Creates and returns a MinIO client using the provided credentials.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO doesn’t enforce regions, but boto3 expects one
    )


def ensure_bucket_exists(client: "boto3.client", bucket_name: str) -> bool:
    """
    Creates a bucket if it does not exist and returns True.
    If the bucket already exists, returns False without raising an exception.

    Args:
        client (boto3.client): MinIO S3 client
        bucket_name (str): Name of the bucket to create

    Returns:
        bool: True if created; otherwise, False
    """
    try:
        client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")
        return True
    except client.exceptions.BucketAlreadyOwnedByYou:
        # Bucket already exists and is owned by you.
        # No need to create it again, so we'll just ignore the exception.
        print(f"Bucket '{bucket_name}' already exists.")
        return False


def upload_file(client: "boto3.client", file_path: str, bucket_name: str) -> None:
    """
    Uploads a specified local file to an S3 bucket on MinIO.

    Args:
        client (boto3.client): MinIO S3 client
        file_path (str): Local path of the file to upload
        bucket_name (str): Name of the target bucket in which to store the uploaded object

    Raises:
        FileNotFoundError: If 'file_path' is not found.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File '{file_path}' does not exist.")

    object_name = os.path.basename(file_path)
    client.upload_file(file_path, bucket_name, object_name)
    print(f"Uploaded '{object_name}' to bucket '{bucket_name}'.")
