{{ config(materialized = 'view', schema = 'int') }}

WITH src AS (          
    SELECT *,
           lower(regexp_replace(dimensions, '[×]', 'x', 'g')) AS dim_std
    FROM {{ ref('stg_room_items') }}
),
split AS (          
    SELECT *,
           trim(split_part(dim_std, 'x', 1)) AS len_raw,
           trim(split_part(dim_std, 'x', 2)) AS wid_raw
    FROM   src
),
digits AS (           
    SELECT *,
           regexp_replace(len_raw, '[^0-9]', '', 'g') AS len_digits,
           regexp_replace(wid_raw, '[^0-9]', '', 'g') AS wid_digits
    FROM split
),
numeric AS (           -- 3 & 4. safe cast → DOUBLE, divide by 100 when needed
    SELECT *,
        CASE
            WHEN len_raw LIKE '%.%'               -- proper decimal already
                 THEN TRY_CAST(regexp_replace(len_raw, '[^0-9\.]', '', 'g') AS DOUBLE)
            WHEN length(len_digits) > 0           -- no dot  assume two-digit precision
                 THEN TRY_CAST(len_digits AS DOUBLE) / 100
            ELSE NULL
        END AS length_m,
        CASE
            WHEN wid_raw LIKE '%.%'
                 THEN TRY_CAST(regexp_replace(wid_raw, '[^0-9\.]', '', 'g') AS DOUBLE)
            WHEN length(wid_digits) > 0
                 THEN TRY_CAST(wid_digits AS DOUBLE) / 100
            ELSE NULL
        END AS width_m
    FROM digits
)
SELECT
    property_id,
    room_index,
    room_name,
    ROUND(length_m, 2) as length_m,
    --|| ' x ' ||  || ' m' AS dimensions_m,
    ROUND(width_m, 2) as width_m,
    dimensions AS dimensions_original,
    room_description
FROM numeric


--select * from staging.stg_room_items
