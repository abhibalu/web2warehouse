CREATE TABLE property_details_delta
ENGINE = DeltaLake(
  'http://<MINIO_ENDPOINT>/delta/delta_table/silver/delta_clean/property_details',
  'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_ENDPOINT_URL', 'AWS_ALLOW_HTTP'
);

CREATE TABLE property_details_room_items_delta
ENGINE = DeltaLake(
  'http://<MINIO_ENDPOINT>/delta/delta_table/silver/delta_clean/property_details_room_items',
  'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_ENDPOINT_URL', 'AWS_ALLOW_HTTP'
);
