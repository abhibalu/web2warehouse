import os
import polars as pl

storage_opts = {
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_ACCESS_KEY"),
    "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT_URL"),
    "AWS_ALLOW_HTTP": "true",
}

paths = {
    "property_details": "s3a://delta/delta_table/silver/delta_clean/property_details",
    "property_details_room_items": "s3a://delta/delta_table/silver/delta_clean/property_details_room_items"
}

for name, path in paths.items():
    df = pl.read_delta(path, storage_options=storage_opts)
    print(f"--- {name} schema ---")
    for col, dt in df.schema.items():
        print(f"{col}: {dt}")
    print()
