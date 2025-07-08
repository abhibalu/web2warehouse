import os
import polars as pl
import json
from deltalake import write_deltalake, DeltaTable
import datetime

from pipelines.helper_functions import read_ndjson_from_minio,clean_accommodation_summary


# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY_ID")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
# MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_URL")

# storage_options = {
#     "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
#     "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
#     "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
#     "AWS_ALLOW_HTTP": "true",
#     "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
# }
# # storage_options_json = json.dumps(storage_options)

# date = datetime.date.today()  - datetime.timedelta(days=3)
# s3_delta_path_stg = f"s3a://delta/delta_table/raw/delta_{date}"
link ="https://www.dng.ie/property-for-sale/3-bedroom-house-for-sale-in-saint-anthonys-26-plunkett-road-finglas-dublin-11-d11t6r0-67b60005c3a41134fa92ada2/"
URL = os.getenv("URL")
JSON_PATH = os.getenv("JSON_PATH")
JSON_SUFFIX = os.getenv("JSON_SUFFIX")

def transform_to_json_url() -> str:
    return link.replace(URL, f"{URL}/{JSON_PATH}") + JSON_SUFFIX

strr = transform_to_json_url()
print(strr)
