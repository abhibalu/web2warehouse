ATTACH TABLE _ UUID '328bbe9a-befd-4980-900c-530d325ac584'
(
    `id` String,
    `room_name` String,
    `dimensions` String,
    `dimensions_alt` String,
    `description` String,
    `ingested_at` String
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192
