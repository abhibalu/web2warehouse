-- Room items table with PRIMARY KEY on id
CREATE TABLE property_details_room_items_mt (
  id String,
  room_name String,
  dimensions String,
  dimensions_alt String,
  description String,
  ingested_at String
)
ENGINE = MergeTree()
ORDER BY id
PRIMARY KEY id;
