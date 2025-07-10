import duckdb
import time


# 1. Connect to DuckDB (file-based or in memory)
con = duckdb.connect("my_data.duckdb")

# 2. Install and load Delta extension
con.sql("INSTALL 'delta';")
con.sql("LOAD 'delta';")

# 3. Create or replace a temporary secret for MinIO via 'config' provider
con.sql("""
CREATE OR REPLACE SECRET minio_s3_secret (
    TYPE S3,
    PROVIDER config,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    USE_SSL false,
    URL_STYLE path
);
""")

# 4. Define Delta table path on MinIO
delta_path = "s3://delta/delta_table/silver/delta_clean"

# 5. Query the Delta table using the secret
result = con.sql(f""" CREATE OR REPLACE TABLE real_estate_clean AS
SELECT * FROM delta_scan('{delta_path}')

;
""")
print("table written")
