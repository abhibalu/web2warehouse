# helper_function.py

import json
import os
import datetime
from dotenv import load_dotenv
from typing import List, Dict
import polars as pl
import re
import uuid


from pipelines.minio_upload import (
    create_minio_client,
)  # use your existing shared function

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

s3 = create_minio_client(endpoint_url, access_key_id, secret_access_key)


def read_ndjson_from_minio(date):
    object_key = OBJECT_NAME_TEMPLATE.format(date=date)
    # print(f'object_key - {object_key}')

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
        existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        if bucket_name in existing_buckets:
            print(f"Bucket '{bucket_name}' already exists.")
            return
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    except Exception as e:
        print(f"Error creating bucket '{bucket_name}': {e}")


def flatten_json(y, prefix=""):
    out = {}

    if isinstance(y, dict):
        for k, v in y.items():
            out.update(flatten_json(v, prefix + k + "."))
    elif isinstance(y, list):
        for i, v in enumerate(y):
            out.update(flatten_json(v, prefix + str(i) + "."))
    else:
        out[prefix[:-1]] = y

    return out


def clean_accommodation_summary(summary: str) -> str:
    if not summary:
        return ""
    lines = summary.replace("\r", "\n").split("\n")
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
            [
                pl.col("address").struct.field(f).alias(f"address_{f}")
                for f in address_fields
            ]
        ).drop("address")

    # Step 2: Clean accommodation summary using apply
    if "accomadation_summary" in df.columns:
        df = df.with_columns(
            [
                pl.col("accomadation_summary")
                .apply(clean_accommodation_summary)
                .alias("accomadation_summary")
            ]
        )

    return df


def transform_to_json_url(link: str) -> str:
    return link.replace(URL, f"{URL}/{JSON_PATH}") + JSON_SUFFIX



def flatten_and_concatenate_address_fields(
    df: pl.DataFrame, struct_col: str = "address"
) -> pl.DataFrame:
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
        pl.concat_str(pl.col(address_cols).fill_null(""), separator=", ").alias(
            struct_col
        )
    )
    return df_with_concat_address.drop(address_cols)


def clean_accommodation_summary_column(
    df: pl.DataFrame,
    column: str = "result.pageContext.propertyData.accomadation_summary",
) -> pl.DataFrame:
    """
    Cleans a multiline string column like 'result.pageContext.propertyData.accomadation_summary' by stripping bullet points and whitespace,
    and joining lines into a comma-separated string.

    Args:
        df (pl.DataFrame): Input DataFrame.
        column (str): The name of the column to clean. Defaults to "result.pageContext.propertyData.accomadation_summary".

    Returns:
        pl.DataFrame: A DataFrame with the cleaned summary column.
    """
    return df.with_columns(
        pl.col(column)
        .map_elements(
            lambda summary: (
                ", ".join(
                    line.strip("•").strip()
                    for line in summary.replace("\r", "\n").split("\n")
                    if line.strip()
                )
                if summary
                else ""
            ),
            return_dtype=pl.Utf8,
        )
        .alias(column)
    )


# ------------------------------------------------------
# Helper functions for Silver-layer extraction
# ------------------------------------------------------
# NOTE: All helpers assume *flattened* column names like
#   result.pageContext.propertyData.images.0.srcUrl
#   result.pageContext.propertyData.room_details.3.name
# ------------------------------------------------------

# ---------- Room Details ----------


# ---------------- room_details ----------------
import polars as pl
import re

"""Helper functions to transform the *flattened* property JSON into tidy Silver-layer tables.
Each function accepts a Polars `DataFrame` (`df`) and returns another `DataFrame` ready to
be appended to a Delta Lake table.

Assumptions
-----------
* Column names follow the dotted paths you shared, e.g.
  `result.pageContext.propertyData.room_details.3.name`.
* `result.pageContext.propertyData._id` is the surrogate key for every property (`property_id`).
* `latitude`/`longitude` columns already exist (added earlier in the pipeline).

All code is Polars ≥ 0.20-style; `DataFrame.unpivot` replaces the deprecated `melt` API.
"""

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------
IMAGE_SUBFIELDS: list[str] = [
    "srcUrl", "url", "caption", "order", "etag", "reapit_etag",
    "last-modified", "createdAt", "updatedAt",
]

# ------------------------------------------------------------------
# 1. explode_room_details
# ------------------------------------------------------------------

def explode_room_details(df: pl.DataFrame) -> pl.DataFrame:
    """Return one row per `property_id` × `room_index` with tidy room fields."""

    # Columns that belong to room_details
    room_cols = [
        c for c in df.columns
        if c.startswith("result.pageContext.propertyData.room_details.")
    ]
    if not room_cols:
        return pl.DataFrame([])

    long = (
        df.select(
            pl.col("result.pageContext.propertyData._id").alias("property_id"),
            *room_cols,
        )
        .unpivot(
            on=room_cols,
            index="property_id",
            variable_name="attribute",
            value_name="value",
        )
        .with_columns(
            [
                # extract numeric index (0,1,2, …) from “…room_details.<idx>.field”
                pl.col("attribute")
                .str.extract(r"room_details\.(\d+)\.")
                .cast(pl.Int64)
                .alias("room_index"),
                # extract the trailing field name (name/dimensions/description/…)
                pl.col("attribute")
                .str.extract(r"room_details\.\d+\.(.*)$")
                .alias("field"),
            ]
        )
        .filter(pl.col("value").is_not_null())
    )

    wide = (
        long
        .pivot(
            index=["property_id", "room_index"],
            columns="field", # type: ignore
            values="value",
            aggregate_function="first",  # if duplicates exist, keep first non-null
        )
        .rename(
            {
                "name": "room_name",
                "dimensions": "dimensions",
                "dimensionsAlt": "dimensions_alt",
                "description": "room_description",
            }
        )
        .sort(["property_id", "room_index"])
    )
    return wide

# ------------------------------------------------------------------
# 2. explode_images
# ------------------------------------------------------------------

def explode_images(df: pl.DataFrame) -> pl.DataFrame:
    """Return one row per `property_id` × `image_index` with image metadata."""

    img_cols = [
        c for c in df.columns
        if c.startswith("result.pageContext.propertyData.images.")
    ]
    if not img_cols:
        return pl.DataFrame([])

    long = (
        df.select(
            pl.col("result.pageContext.propertyData._id").alias("property_id"),
            *img_cols,
        )
        .unpivot(
            on=img_cols,
            index="property_id",
            variable_name="attribute",
            value_name="value",
        )
        .with_columns(
            [
                pl.col("attribute")
                .str.extract(r"images\.(\d+)\.")
                .cast(pl.Int64)
                .alias("image_index"),
                pl.col("attribute")
                .str.extract(r"images\.\d+\.(.*)$")
                .alias("field"),
            ]
        )
        .filter(
            (pl.col("value").is_not_null())
            & (pl.col("field").is_in(IMAGE_SUBFIELDS))
        )
    )

    wide = (
        long
        .pivot(
            index=["property_id", "image_index"],
            columns="field", # type: ignore
            values="value",
            aggregate_function="first",
        )
        .rename(
            {
                "srcUrl": "src_url",
                "url": "full_url",
                "caption": "caption",
                "order": "image_order",
                "etag": "etag",
                "reapit_etag": "reapit_etag",
                "last-modified": "last_modified",
                "createdAt": "created_at",
                "updatedAt": "updated_at",
            }
        )
        .sort(["property_id", "image_index"])
    )
    return wide

# ------------------------------------------------------------------
# 3. extract_agent_dim
# ------------------------------------------------------------------

def extract_agent_dim(df: pl.DataFrame) -> pl.DataFrame:
    """Deduplicated list of negotiators/agents."""
    return (
        df.select(
            pl.col("result.pageContext.propertyData.crm_negotiator_id.ID").alias("agent_id"),
            pl.col("result.pageContext.propertyData.crm_negotiator_id.Name").alias("agent_name"),
            pl.col("result.pageContext.propertyData.crm_negotiator_id.Email").alias("agent_email"),
        )
        .filter(pl.col("agent_id").is_not_null())
        .unique(subset=["agent_id"])
    )

# ------------------------------------------------------------------
# 4. extract_location_dim
# ------------------------------------------------------------------

def extract_location_dim(df: pl.DataFrame) -> pl.DataFrame:
    """Unique set of locations with lat/lon."""
    return (
        df.select(
            pl.col("result.pageContext.propertyData.address.house_number").alias("house_number"),
            pl.col("result.pageContext.propertyData.address.address1").alias("address_line1"),
            pl.col("result.pageContext.propertyData.address.address2").alias("address_line2"),
            pl.col("result.pageContext.propertyData.address.address3").alias("address_line3"),
            pl.col("result.pageContext.propertyData.address.address4").alias("address_line4"),
            pl.col("result.pageContext.propertyData.address.country").alias("country"),
            pl.col("result.pageContext.propertyData.address.postcode").alias("postcode"),
            pl.col("latitude"),
            pl.col("longitude"),
        ).unique()
    )

# ------------------------------------------------------------------
# 5. extract_energy_metrics
# ------------------------------------------------------------------

def extract_energy_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Energy ratings and related metrics per property snapshot.

    * Strips non‑numeric characters from `pEPI` (e.g. "61.51 kWh/m²/yr") so it can be
      safely cast to `Float64`.  If nothing numeric is found the value becomes `null`.
    """
    return (
        df.select(
            pl.col("result.pageContext.propertyData._id").alias("property_id"),
            pl.col("result.pageContext.propertyData.extras.extrasField.pBERRating").alias("pber_rating"),
            pl.col("result.pageContext.propertyData.extras.extrasField.pBERNumber").alias("pber_number"),
            # extract numeric portion before casting to float
            pl.col("result.pageContext.propertyData.extras.extrasField.pEPI")
              .str.replace_all(r"[^0-9.]", "")
              .cast(pl.Float64, strict=False)
              .alias("epi"),
            pl.col("result.pageContext.propertyData.extras.extrasField.ratingValue").alias("rating_value"),
            pl.col("result.pageContext.propertyData.extras.created_on").alias("recorded_at"),
        )
        .filter(pl.col("property_id").is_not_null())
    )