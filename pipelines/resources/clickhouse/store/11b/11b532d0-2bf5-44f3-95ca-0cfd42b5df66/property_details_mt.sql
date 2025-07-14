ATTACH TABLE _ UUID '469dcc17-108b-4fdc-b54f-52ae330e1628'
(
    `listed_title` String,
    `id` String,
    `location` String,
    `bedroom` Int64,
    `price` Int64,
    `latitude` Float64,
    `house_name` String,
    `house_number` String,
    `country` String,
    `postcode` String,
    `longitude` Float64,
    `bathroom` Int64,
    `reception` Int64,
    `floorarea_min` Int64,
    `accomadation_summary` String,
    `status` String,
    `address` String,
    `ingested_at` String
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192
