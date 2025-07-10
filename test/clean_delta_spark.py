import polars as pl

# Load your NDJSON
df = pl.read_ndjson("scraped_data/properties.ndjson")

# Explode room_details, handling nulls by replacing with empty list
df = df.with_columns([
    pl.when(pl.col("room_details").is_null())
      .then(pl.lit([]))
      .otherwise(pl.col("room_details"))
      .alias("room_details")
])

# Explode the list of structs
exploded = df.explode("room_details")

# Add index to keep track of room position per property
exploded = exploded.with_row_count("room_index")

# Select & flatten fields from room_details struct
room_details_flat = exploded.select([
    pl.col("id").alias("property_id"),
    pl.col("room_index"),
    pl.col("room_details").struct.field("name").alias("room_name"),
    pl.col("room_details").struct.field("dimensions"),
    pl.col("room_details").struct.field("dimensionsAlt"),
    pl.col("room_details").struct.field("description"),
])

print(room_details_flat)
