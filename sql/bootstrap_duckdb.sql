-- sql/bootstrap_duckdb.sql  (works with DuckDB ≥ 0.10.3/1.3)

--------------------------------------------------------------------
-- 0. Extensions
--------------------------------------------------------------------
INSTALL httpfs;  LOAD httpfs;
INSTALL delta;   LOAD delta;

--------------------------------------------------------------------
-- 1. MinIO credentials (CREATE SECRET is the cleanest way)
--------------------------------------------------------------------
CREATE PERSISTENT SECRET minio (
  TYPE      s3,
  KEY_ID    'minioadmin',
  SECRET    'minioadmin',
  REGION    'us-east-1',
  ENDPOINT  '127.0.0.1:9000',  -- no “http://”
  URL_STYLE 'path',            -- MinIO prefers path-style
  USE_SSL   FALSE              -- = plain HTTP
);

--------------------------------------------------------------------
-- 2. External Delta views
--------------------------------------------------------------------
CREATE OR REPLACE VIEW raw_property_json AS
SELECT * FROM delta_scan('s3://delta/delta_table/raw/delta_2025-07-18');

CREATE OR REPLACE VIEW stg_property_flat  AS
SELECT * FROM delta_scan('s3://delta/delta_table/silver/property_details');

CREATE OR REPLACE VIEW stg_room_items     AS
SELECT * FROM delta_scan('s3://delta/delta_table/silver/property_rooms');

CREATE OR REPLACE VIEW stg_image_items    AS
SELECT * FROM delta_scan('s3://delta/delta_table/silver/property_images');

CREATE OR REPLACE VIEW stg_agent_dim      AS
SELECT * FROM delta_scan('s3://delta/delta_table/silver/agents');

CREATE OR REPLACE VIEW stg_location_dim   AS
SELECT * FROM delta_scan('s3://delta/delta_table/silver/locations');

CREATE OR REPLACE VIEW stg_energy_metrics AS
SELECT * FROM delta_scan('s3://delta/delta_table/silver/property_energy');
