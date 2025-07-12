import os
import polars as pl
from deltalake import DeltaTable

# Set your MinIO S3 storage options
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

# Delta table paths
s3_delta_path_property = "s3a://delta/delta_table/silver/delta_clean/property_details"
s3_delta_path_room_items = "s3a://delta/delta_table/silver/delta_clean/property_details_room_items"

# Local output folder
output_folder = "test"
os.makedirs(output_folder, exist_ok=True)

def export_delta_to_csv(s3_path, local_filename):
    try:
        df = pl.read_delta(s3_path, storage_options=storage_options)
        output_path = os.path.join(output_folder, local_filename)
        df.write_csv(output_path)
        print(f"✅ Exported to: {output_path} ({df.shape[0]} rows)")
    except Exception as e:
        print(f"❌ Failed to export from {s3_path}: {e}")

if __name__ == "__main__":
    export_delta_to_csv(s3_delta_path_property, "property_details.csv")
    export_delta_to_csv(s3_delta_path_room_items, "property_details_room_items.csv")
