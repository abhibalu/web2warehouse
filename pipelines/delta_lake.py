# delta_lake.py
import os
import polars as pl
from deltalake import write_deltalake, DeltaTable
from datetime import date, datetime, timezone

from pipelines.helper_functions import (
    read_ndjson_from_minio,
    clean_accommodation_summary_column,
    explode_room_details,
    explode_images,
    extract_agent_dim,
    extract_location_dim,
    extract_energy_metrics,
)

# Environment and storage setup
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_URL")

storage_options = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# Paths
dt = date.today()
RAW_PATH = f"s3a://delta/delta_table/raw/delta_{dt}"
STG_PROP_PATH = "s3a://delta/delta_table/silver/property_details"
STG_ROOMS_PATH = "s3a://delta/delta_table/silver/property_rooms"
STG_IMAGES_PATH = "s3a://delta/delta_table/silver/property_images"
STG_AGENT_PATH = "s3a://delta/delta_table/silver/agents"
STG_LOCATION_PATH = "s3a://delta/delta_table/silver/locations"
STG_ENERGY_PATH = "s3a://delta/delta_table/silver/property_energy"


def ingest_raw(date_str: date):
    records = read_ndjson_from_minio(date_str)
    if not records:
        print(f"No data to ingest for {date_str}")
        return
    df = pl.DataFrame(records)

    # Drop columns with only nulls
    null_cols = [c for c, dt in zip(df.columns, df.dtypes) if dt == pl.Null]
    if null_cols:
        df = df.drop(null_cols)
        print(f"Dropped null-only columns: {null_cols}")

    # Cast ambiguous columns to string
    for col, dt in zip(df.columns, df.dtypes):
        if dt == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    write_deltalake(
        RAW_PATH,
        df,
        storage_options=storage_options,
        mode="append",
        schema_mode="merge",
    )
    print(f"✅ Raw delta table ingested for {date_str}")


def build_silver_incremental():
    raw_df = pl.read_delta(RAW_PATH, storage_options=storage_options)
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        existing = pl.read_delta(STG_PROP_PATH, storage_options=storage_options)
        existing_ids = existing.select("id").unique()
        new_df = raw_df.filter(
            ~pl.col("result.pageContext.propertyData._id").is_in(existing_ids["id"])
        )
    except Exception:
        new_df = raw_df

    if new_df.is_empty():
        print("✅ No new records to process.")
        return

    # Manually flatten address fields and concatenate
    cleaned = new_df.with_columns(
        [
            pl.concat_str(
                [
                    pl.col("result.pageContext.propertyData.address.house_number"),
                    pl.lit(" "),
                    pl.col("result.pageContext.propertyData.address.address1"),
                    pl.lit(", "),
                    pl.col("result.pageContext.propertyData.address.address2"),
                    pl.lit(", "),
                    pl.col("result.pageContext.propertyData.address.address3"),
                ],
                separator="",
            ).alias("full_address"),
            pl.col("result.pageContext.propertyData.latitude").alias("latitude"),
            pl.col("result.pageContext.propertyData.longitude").alias("longitude"),
        ]
    )
    cleaned = clean_accommodation_summary_column(cleaned)
    cleaned = cleaned.with_columns(pl.lit(now_ts).alias("ingested_at"))

    # Extract silver tables
    prop_df = (
        cleaned
        .drop([
            col for col in cleaned.columns
            if col.startswith("result.pageContext.propertyData.address.")
            or col in {
                "result.pageContext.propertyData.room_details",
                "result.pageContext.propertyData.images",
                "result.pageContext.propertyData.extras"
            }
        ])
        .with_columns(pl.col("result.pageContext.propertyData._id").alias("id"))
    )

    
    room_df = explode_room_details(cleaned).with_columns(pl.lit(now_ts).alias("ingested_at"))
    img_df = explode_images(cleaned).with_columns(pl.lit(now_ts).alias("ingested_at"))
    agent_df = extract_agent_dim(cleaned).with_columns(pl.lit(now_ts).alias("loaded_at"))
    loc_df = extract_location_dim(cleaned).with_columns(pl.lit(now_ts).alias("loaded_at"))
    energy_df = extract_energy_metrics(cleaned).with_columns(pl.lit(now_ts).alias("loaded_at"))
    # print(f'cols - prop_details count :{len(prop_df.columns)}')
    
    for path, df in [
        (STG_PROP_PATH, prop_df),
        (STG_ROOMS_PATH, room_df),
        (STG_IMAGES_PATH, img_df),
        (STG_AGENT_PATH, agent_df),
        (STG_LOCATION_PATH, loc_df),
        (STG_ENERGY_PATH, energy_df),
    ]:
        write_deltalake(
            path,
            df,
            storage_options=storage_options,
            mode="append",
            schema_mode="merge",
        )
        print(f"✅ Appended {df.shape[0]} rows to {path}")


if __name__ == "__main__":
    ingest_raw(dt)
    build_silver_incremental()
