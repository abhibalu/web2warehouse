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
RAW_PATH = "s3a://delta/delta_table/raw/properties"  # Single append-only raw table
STG_PROP_PATH = "s3a://delta/delta_table/silver/property_details"
STG_ROOMS_PATH = "s3a://delta/delta_table/silver/property_rooms"
STG_IMAGES_PATH = "s3a://delta/delta_table/silver/property_images"
STG_AGENT_PATH = "s3a://delta/delta_table/silver/agents"
STG_LOCATION_PATH = "s3a://delta/delta_table/silver/locations"
STG_ENERGY_PATH = "s3a://delta/delta_table/silver/property_energy"
WATERMARK_PATH = "s3a://delta/delta_table/_checkpoints/silver_watermark"  # Track last processed timestamp


def upsert_to_delta(path: str, df: pl.DataFrame, key_cols: list[str]):
    """Idempotent upsert into a Delta table using key columns.
    Falls back to create table if it doesn't exist.
    """
    if df.is_empty():
        print(f"No data to upsert into {path}")
        return
    try:
        table = DeltaTable(path, storage_options=storage_options)
        src = df.to_arrow()
        predicate = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
        (
            table.merge(
                source=src,
                predicate=predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        print(f"✓ Upserted {df.shape[0]} rows into {path}")
    except Exception as e:
        write_deltalake(
            path,
            df,
            storage_options=storage_options,
            mode="overwrite",
            schema_mode="merge",
        )
        print(f"✓ Created Delta table at {path} with {df.shape[0]} rows (reason: {e})")


def get_watermark() -> str:
    """Retrieve the last processed timestamp for incremental silver loads."""
    try:
        wm_df = pl.read_delta(WATERMARK_PATH, storage_options=storage_options)
        return wm_df.select("watermark").item()
    except Exception:
        return "1970-01-01T00:00:00+00:00"  # Epoch if no checkpoint exists


def save_watermark(timestamp: str):
    """Save the new watermark after successful silver processing."""
    wm_df = pl.DataFrame({"watermark": [timestamp]})
    write_deltalake(
        WATERMARK_PATH,
        wm_df,
        storage_options=storage_options,
        mode="overwrite",
    )
    print(f"✓ Watermark updated: {timestamp}")


def ingest_raw(date_str: date):
    """Incrementally load staging NDJSON into raw Delta table.
    Deduplicates by property_id to prevent re-ingesting same staging files.
    """
    records = read_ndjson_from_minio(date_str)
    if not records:
        print(f"No data to ingest for {date_str}")
        return
    df = pl.DataFrame(records)

    # Drop columns with only nulls
    null_cols = [c for c, dt_ in zip(df.columns, df.dtypes) if dt_ == pl.Null]
    if null_cols:
        df = df.drop(null_cols)
        print(f"Dropped null-only columns: {null_cols}")

    # Cast ambiguous columns to string
    for col, dt_ in zip(df.columns, df.dtypes):
        if dt_ == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    now_ts = datetime.now(timezone.utc).isoformat()
    df = df.with_columns([
        pl.col("result.pageContext.propertyData._id").alias("property_id"),
        pl.lit(now_ts).alias("ingested_at"),
        pl.lit(str(date_str)).alias("source_date"),  # Track staging file date
    ])

    # Deduplicate within this batch by property_id
    df = df.unique(subset=["property_id"], keep="last")

    # Check existing raw to avoid re-ingesting same property_ids
    try:
        existing_raw = pl.read_delta(RAW_PATH, storage_options=storage_options)
        existing_ids = existing_raw.get_column("property_id").unique().to_list()
        df = df.filter(~pl.col("property_id").is_in(existing_ids))
        print(f"Filtered {len(existing_ids)} existing property_ids from batch")
    except Exception:
        pass  # No existing raw table, proceed

    if df.is_empty():
        print(f"No new records to append to raw (all already exist)")
        return

    # Append to RAW Delta table
    try:
        _ = DeltaTable(RAW_PATH, storage_options=storage_options)
        write_deltalake(
            RAW_PATH,
            df,
            storage_options=storage_options,
            mode="append",
            schema_mode="merge",
        )
        print(f"✓ Appended {df.shape[0]} new records to raw")
    except Exception as e:
        write_deltalake(
            RAW_PATH,
            df,
            storage_options=storage_options,
            mode="overwrite",
            schema_mode="merge",
        )
        print(f"✓ Created raw Delta table with {df.shape[0]} records (reason: {e})")


def build_silver_incremental():
    """Incrementally process raw → silver using watermark-based checkpointing.
    Only processes raw records with ingested_at > last watermark.
    """
    last_watermark = get_watermark()
    print(f"Processing raw records since: {last_watermark}")

    # Read only new raw records since last watermark
    raw_df = pl.read_delta(RAW_PATH, storage_options=storage_options)
    new_raw = raw_df.filter(pl.col("ingested_at") > last_watermark)

    if new_raw.is_empty():
        print("No new raw records to process for silver")
        return

    print(f"Processing {new_raw.shape[0]} new raw records")
    now_ts = datetime.now(timezone.utc).isoformat()

    # Manually flatten address fields and concatenate
    cleaned = new_raw.with_columns(
        [
            pl.concat_str(
                [
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

    # Extract silver tables (idempotent upserts)
    prop_df = (
        cleaned
        .drop([
            col for col in cleaned.columns
            if col.startswith("result.pageContext.propertyData.address.")
            or col in {
                "result.pageContext.propertyData.room_details",
                "result.pageContext.propertyData.images",
                "result.pageContext.propertyData.extras",
            }
        ])
        .with_columns(pl.col("result.pageContext.propertyData._id").alias("id"))
    )

    room_df = (
        explode_room_details(cleaned)
        .with_columns([
            pl.lit(now_ts).alias("ingested_at"),
            (pl.concat_str([
                pl.col("property_id"), pl.lit("_"), pl.col("room_index").cast(pl.Utf8)
            ])).alias("id"),
        ])
        .rename({"property_id": "prop_id", "room_description": "description"})
    )

    img_df = (
        explode_images(cleaned)
        .with_columns([
            pl.lit(now_ts).alias("ingested_at"),
            (pl.concat_str([
                pl.col("property_id"), pl.lit("_"), pl.col("image_index").cast(pl.Utf8)
            ])).alias("id"),
        ])
    )

    agent_df = extract_agent_dim(cleaned).with_columns(pl.lit(now_ts).alias("loaded_at"))

    loc_df = (
        extract_location_dim(cleaned)
        .with_columns([
            (pl.concat_str([
                pl.col("address_line1").fill_null("").cast(pl.Utf8), pl.lit("|"),
                pl.col("address_line2").fill_null("").cast(pl.Utf8), pl.lit("|"),
                pl.col("address_line3").fill_null("").cast(pl.Utf8), pl.lit("|"),
                pl.col("address_line4").fill_null("").cast(pl.Utf8), pl.lit("|"),
                pl.col("postcode").fill_null("").cast(pl.Utf8), pl.lit("|"),
                pl.col("latitude").cast(pl.Utf8), pl.lit("|"), pl.col("longitude").cast(pl.Utf8)
            ])).alias("location_key"),
            pl.lit(now_ts).alias("loaded_at"),
        ])
    )

    energy_df = extract_energy_metrics(cleaned).with_columns(pl.lit(now_ts).alias("loaded_at"))

    # Idempotent upserts into Delta silver tables
    upsert_to_delta(STG_PROP_PATH, prop_df, ["id"])
    upsert_to_delta(STG_ROOMS_PATH, room_df, ["id"])  # stable synthetic id
    upsert_to_delta(STG_IMAGES_PATH, img_df, ["id"])  # stable synthetic id
    upsert_to_delta(STG_AGENT_PATH, agent_df, ["agent_id"])  # natural key
    upsert_to_delta(STG_LOCATION_PATH, loc_df, ["location_key"])  # composite natural key
    upsert_to_delta(STG_ENERGY_PATH, energy_df, ["property_id", "recorded_at"])  # snapshot key

    # Update watermark to latest ingested_at from this batch
    new_watermark = new_raw.select(pl.col("ingested_at").max()).item()
    save_watermark(new_watermark)


if __name__ == "__main__":
    ingest_raw(dt)
    build_silver_incremental()
