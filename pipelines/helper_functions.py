# helper_function.py

import json
import os
import datetime
from dotenv import load_dotenv
from typing import List, Dict
import polars as pl
import re

from pipelines.minio_upload import create_minio_client  # use your existing shared function
load_dotenv()

date = datetime.date.today()

URL = os.getenv("URL")
JSON_PATH = os.getenv("JSON_PATH")
JSON_SUFFIX = os.getenv("JSON_SUFFIX")

BUCKET_NAME = os.getenv("BUCKET_NAME")
OBJECT_NAME_TEMPLATE = os.getenv("OBJECT_NAME_TEMPLATE")


endpoint_url = os.getenv("MINIO_ENDPOINT_URL")
access_key_id = os.getenv("MINIO_ACCESS_KEY_ID")
secret_access_key = os.getenv("MINIO_SECRET_ACCESS_KEY")

s3 = create_minio_client(endpoint_url,access_key_id,secret_access_key)

def read_ndjson_from_minio(date):
    object_key = OBJECT_NAME_TEMPLATE.format(date=date)
    print(f'object_key - {object_key}')

    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=object_key)
        content = response["Body"].read().decode("utf-8")
        json_lines = [json.loads(line) for line in content.strip().splitlines()]
        return json_lines
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"Object not found: {object_key}")
    except Exception as e:
        raise RuntimeError(f"Failed to read from MinIO: {e}")
    
def create_bucket(bucket_name: str):
    
    try:
        # Check if bucket already exists by listing buckets
        existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
        if bucket_name in existing_buckets:
            print(f"Bucket '{bucket_name}' already exists.")
            return
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating bucket '{bucket_name}': {e}")

    
def clean_accommodation_summary(summary: str) -> str:
    if not summary:
        return ""
    lines = summary.replace('\r', '\n').split('\n')
    cleaned = [line.strip("• ").strip() for line in lines if line.strip()]
    return ", ".join(cleaned)

def flatten_property_data_polars(df: pl.DataFrame) -> pl.DataFrame:
    """
    Flatten struct fields (like 'address') and clean up accommodation summary.

    Args:
        df (pl.DataFrame): Polars DataFrame with nested columns.

    Returns:
        pl.DataFrame: Flattened and cleaned Polars DataFrame.
    """

    # Step 1: Unpack address struct (if it exists)
    if "address" in df.columns:
        address_fields = df.select("address").struct.fields
        df = df.with_columns(
            [pl.col("address").struct.field(f).alias(f"address_{f}") for f in address_fields]
        ).drop("address")

    # Step 2: Clean accommodation summary using apply
    if "accomadation_summary" in df.columns:
        df = df.with_columns([
            pl.col("accomadation_summary").apply(clean_accommodation_summary).alias("accomadation_summary")
        ])

    return df


def transform_to_json_url(link: str) -> str:
    return link.replace(URL, f"{URL}/{JSON_PATH}") + JSON_SUFFIX


def extract_required_fields(data: dict) -> Dict:

    return {
    "listed_title": data["result"]["pageContext"]["propertyData"]["title"],
    "id" : data["result"]["pageContext"]["propertyData"]["id"],
    "location": data["result"]["pageContext"]["propertyData"]["display_address"],
    "bedroom": data["result"]["pageContext"]["propertyData"]["bedroom"],
    "price": data["result"]["pageContext"]["propertyData"]["price"],
    "latitude": data["result"]["pageContext"]["propertyData"]["latitude"] if data["result"]["pageContext"]["propertyData"]["latitude"] else None,
    "address" : data["result"]["pageContext"]["propertyData"]["address"],
    "longitude": data["result"]["pageContext"]["propertyData"]["longitude"] if data["result"]["pageContext"]["propertyData"]["longitude"] else None,
    "bathroom": data["result"]["pageContext"]["propertyData"]["bathroom"],
    "reception": data["result"]["pageContext"]["propertyData"]["reception"],
    "floorarea_min": data["result"]["pageContext"]["propertyData"]["floorarea_min"],
    "accomadation_summary" : data["result"]["pageContext"]["propertyData"]["accomadation_summary"] if data["result"]["pageContext"]["propertyData"]["accomadation_summary"] else None,
    "room_details" : data["result"]["pageContext"]["propertyData"]["room_details"] if data["result"]["pageContext"]["propertyData"]["room_details"] else None,
    "status": data["result"]["pageContext"]["propertyData"]["status"] if data["result"]["pageContext"]["propertyData"]["status"] else None
}

def flatten_and_concatenate_address_fields(df: pl.DataFrame, struct_col: str = "address") -> pl.DataFrame:
    """
    Unnests the 'address' struct column and concatenates any 'addressX' fields into a single 'address' column.

    Args:
        df (pl.DataFrame): The input DataFrame containing a struct column for address.
        struct_col (str): The name of the struct column to unnest. Defaults to "address".

    Returns:
        pl.DataFrame: A DataFrame with a concatenated 'address' column.
    """
    df_unnested = df.unnest(struct_col)
    address_cols = [col for col in df_unnested.columns if re.match(r"address\d+$", col)]

    df_with_concat_address = df_unnested.with_columns(
        pl.concat_str(
            pl.col(address_cols).fill_null(""),
            separator=", "
        ).alias(struct_col)
    )
    return df_with_concat_address.drop(address_cols)

def clean_accommodation_summary_column(df: pl.DataFrame, column: str = "accomadation_summary") -> pl.DataFrame:
    """
    Cleans a multiline string column like 'accomadation_summary' by stripping bullet points and whitespace,
    and joining lines into a comma-separated string.

    Args:
        df (pl.DataFrame): Input DataFrame.
        column (str): The name of the column to clean. Defaults to "accomadation_summary".

    Returns:
        pl.DataFrame: A DataFrame with the cleaned summary column.
    """
    return df.with_columns(
        pl.col(column).map_elements(
            lambda summary: ", ".join(
                line.strip("•").strip()
                for line in summary.replace('\r', '\n').split('\n')
                if line.strip()
            ) if summary else "",
            return_dtype=pl.Utf8
        ).alias(column)
    )

def explode_room_details(df: pl.DataFrame) -> pl.DataFrame:
    exploded = df.select(["id", "room_details"]).explode("room_details")

    return exploded.select([
        pl.col("id"),
        pl.col("room_details").struct.field("name").alias("room_name"),
        pl.col("room_details").struct.field("dimensions").alias("dimensions"),
        pl.col("room_details").struct.field("dimensionsAlt").alias("dimensions_alt"),
        pl.col("room_details").struct.field("description").alias("description"),
    ])
