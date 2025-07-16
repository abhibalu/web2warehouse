import duckdb
import time


# 1. Connect to DuckDB (file-based or in memory)

def main():
    con = duckdb.connect("/Users/abhijithm/Documents/Code/Scrapping/real_estate_pipeline/dbt/real_estate/my_data.duckdb")

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
    property_details_room_items = "s3://delta/delta_table/silver/delta_clean/property_details_room_items"
    property_details = "s3://delta/delta_table/silver/delta_clean/property_details"


    count_pre_ing_property_details = con.sql("select count(*) from property_details_mt;").fetchone()[0]
    count_pre_ing_property_details_room = con.sql("select count(*) from property_details_room_items_mt;").fetchone()[0]

    con.sql(f"""
    INSERT INTO property_details_mt (
    listed_title, id, location, bedroom, price, latitude, house_name,
    house_number, country, postcode, longitude, bathroom, reception,
    floorarea_min, accomadation_summary, status, address, ingested_at
    )
    SELECT
    listed_title, id, location, bedroom, price, latitude, house_name,
    house_number, country, postcode, longitude, bathroom, reception,
    floorarea_min, accomadation_summary, status, address, ingested_at
    FROM delta_scan('{property_details}')
    ON CONFLICT (id)
    DO UPDATE SET
    listed_title = EXCLUDED.listed_title,
    location = EXCLUDED.location,
    bedroom = EXCLUDED.bedroom,
    price = EXCLUDED.price,
    latitude = EXCLUDED.latitude,
    house_name = EXCLUDED.house_name,
    house_number = EXCLUDED.house_number,
    country = EXCLUDED.country,
    postcode = EXCLUDED.postcode,
    longitude = EXCLUDED.longitude,
    bathroom = EXCLUDED.bathroom,
    reception = EXCLUDED.reception,
    floorarea_min = EXCLUDED.floorarea_min,
    accomadation_summary = EXCLUDED.accomadation_summary,
    status = EXCLUDED.status,
    address = EXCLUDED.address,
    ingested_at = EXCLUDED.ingested_at;
    """)

    con.sql(f"""
    INSERT INTO property_details_room_items_mt (
    id,
    prop_id,
    room_name,
    dimensions,
    dimensions_alt,
    description,
    ingested_at
    )
    SELECT
    id,
    prop_id,
    room_name,
    dimensions,
    dimensions_alt,
    description,
    ingested_at
    FROM delta_scan('{property_details_room_items}')
    ON CONFLICT (id)
    DO UPDATE SET
    prop_id = EXCLUDED.prop_id,
    room_name = EXCLUDED.room_name,
    dimensions = EXCLUDED.dimensions,
    dimensions_alt = EXCLUDED.dimensions_alt,
    description = EXCLUDED.description,
    ingested_at = EXCLUDED.ingested_at;
    """)
    count_post_ing_property_details = con.sql("select count(*) from property_details_mt;").fetchone()[0]
    count_post_ing_property_details_room = con.sql("select count(*) from property_details_room_items_mt;").fetchone()[0]

    diff_prop = count_post_ing_property_details - count_pre_ing_property_details
    diff_prop_rooms = count_post_ing_property_details_room - count_pre_ing_property_details_room


    print("the warehouse is updated from delta")
    print(f"property table added - {diff_prop} rows ")
    print(f"property_rooms table added - {diff_prop_rooms} rows ")

if __name__ == "__main__" :
    main()