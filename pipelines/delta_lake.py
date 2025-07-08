# delta_lake.py
import os
import polars as pl
import json
from deltalake import write_deltalake, DeltaTable
import datetime
import re

from pipelines.helper_functions import read_ndjson_from_minio,flatten_and_concatenate_address_fields,clean_accommodation_summary_column
# delta = create_bucket("delta")  # path to store your Delta Lake table

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_URL")

storage_options = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
}
# storage_options_json = json.dumps(storage_options)

date = datetime.date.today()  - datetime.timedelta(days=3)
s3_delta_path_stg = f"s3a://delta/delta_table/raw/delta_{date}"
s3_delta_path_intermediate = f"s3a://delta/delta_table/silver/delta_clean"

def create_or_update_delta_lake_stg(date):
    # Step 1: Read JSON records from MinIO
    records = read_ndjson_from_minio(date)
    
    # flatten_records = [flatten_property_data(record) for record in records]
    # print(flatten_records)
    if not records:
        print(f"No data found for date range {date}.")
        return

    # Step 2: Convert to Polars DataFrame
    df = pl.DataFrame(records)

    # Step 3: Write to Delta Lake
    write_deltalake(s3_delta_path_stg, df,storage_options=storage_options,mode="overwrite")
    print(f"✅ Delta Lake updated with records on {date}")


def read_delta_lake():
    # Read Delta table as Polars DataFrame
    dt = DeltaTable(s3_delta_path_stg,storage_options=storage_options)
    return dt



def create_intermediate_clean_layer():
    # Step 1: Load raw Delta table from staging
    df = pl.read_delta(s3_delta_path_stg,storage_options=storage_options)

    # Step 2: Flatten & clean via Polars-native function
    cleaned_df = flatten_and_concatenate_address_fields(df)
    cleaned_df = clean_accommodation_summary_column(cleaned_df)

    # Step 3: Write to the silver/intermediate Delta table
    write_deltalake(
        s3_delta_path_intermediate,
        cleaned_df,
        storage_options=storage_options,
        mode="overwrite",
        schema_mode='overwrite'
    )
    
    print("✅ Silver layer (cleaned) Delta table written successfully.")



# Example usage
if __name__ == "__main__":
    create_or_update_delta_lake_stg(date)
    create_intermediate_clean_layer()      # silver layer
    
    dt = DeltaTable(s3_delta_path_intermediate, storage_options=storage_options)
    schema = dt.schema().to_arrow()
    print(f"Silver Table Schema: {schema}")
    print(f"Version: {dt.version()}")
    print(f"Files: {dt.files()}")
