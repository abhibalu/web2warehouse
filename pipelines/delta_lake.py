# delta_lake.py
import os
import polars as pl
import json
from deltalake import write_deltalake, DeltaTable
import datetime
from datetime import datetime, date, timezone,timedelta
import re

from pipelines.helper_functions import read_ndjson_from_minio,flatten_and_concatenate_address_fields,clean_accommodation_summary_column,explode_room_details
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

date = date.today()

s3_delta_path_stg = f"s3a://delta/delta_table/raw/delta_{date}"
s3_delta_path_intermediate = f"s3a://delta/delta_table/silver/delta_clean"
s3_delta_path_property = f"s3a://delta/delta_table/silver/delta_clean/property_details" 
s3_delta_path_room_items = f"s3a://delta/delta_table/silver/delta_clean/property_details_room_items" 

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
    write_deltalake(s3_delta_path_stg, df,storage_options=storage_options,mode="overwrite",schema_mode='overwrite')
    print(f"✅ Delta Lake updated with records on {date}")


def read_delta_lake():
    # Read Delta table as Polars DataFrame
    dt = DeltaTable(s3_delta_path_stg,storage_options=storage_options)
    return dt



def create_intermediate_clean_layer_incremental():
    # Step 1: Load staging and intermediate data
    stg_df = pl.read_delta(s3_delta_path_stg, storage_options=storage_options)

    try:
        intermediate_df = pl.read_delta(s3_delta_path_property, storage_options=storage_options)
    except Exception:
        print("⚠️ Property intermediate layer not found. Assuming first run.")
        intermediate_df = pl.DataFrame([])

    key_col = "id"
    if intermediate_df.is_empty():
        new_records_df = stg_df
    else:
        existing_keys = intermediate_df.select(key_col).unique()
        new_records_df = stg_df.filter(~pl.col(key_col).is_in(existing_keys[key_col]))

    if new_records_df.is_empty():
        print("✅ No new records to process.")
        return

    # Step 2: Clean the new records
    cleaned_df = flatten_and_concatenate_address_fields(new_records_df)
    cleaned_df = clean_accommodation_summary_column(cleaned_df)

    # Step 3: Add ingestion timestamp
    now_utc = datetime.now(timezone.utc).isoformat()
    cleaned_df = cleaned_df.with_columns(
        pl.lit(now_utc).alias("ingested_at")
    )

    # Step 4: Split into property and room_details tables
    property_df = cleaned_df.drop("room_details")

    room_items_df = explode_room_details(cleaned_df).with_columns(
        pl.lit(now_utc).alias("ingested_at")
    )
    print(f"\n\n hellow wrold")
    # Step 5: Write both dataframes incrementally
    write_deltalake(
        s3_delta_path_property,
        property_df,
        storage_options=storage_options,
        mode="append",
        schema_mode="merge",
    )
    write_deltalake(
        s3_delta_path_room_items,
        room_items_df,
        storage_options=storage_options,
        mode="append",
        schema_mode="merge"
    )
    print(f"pipeline for delta is completed.")
    print(f"✅ Appended {property_df.shape[0]} property records.")
    print(f"✅ Appended {room_items_df.shape[0]} room detail records.")



# Example usage
if __name__ == "__main__":
    create_or_update_delta_lake_stg(date)
    create_intermediate_clean_layer_incremental()      # silver layer
    
    dt = DeltaTable(s3_delta_path_room_items, storage_options=storage_options)
    schema = dt.schema().to_arrow()
    num_records = dt.to_pyarrow_table().num_rows
    print(f"Silver Table Schema: {schema}")
    print(f"Number of Records: {num_records}")
    print(f"Version: {dt.version()}")
    print(f"Files: {dt.files()}")
