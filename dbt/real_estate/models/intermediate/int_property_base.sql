{{ config(
    materialized = 'table', 
    schema = 'intermediate',
    on_schema_change = 'append_new_columns') 
    }}

with source as (
    select * from {{ ref('stg_property_flat')}}
),
dedup as(
    select *
    from source
    qualify row_number() over(
        partition by property_id, crm_id, path
        order by ingested_at desc
    ) = 1
)
select 
    property_id,
    path,
    crm_id,
    latitude,
    longitude,
    slug,
    display_address,
    town,
    postcode,
    area,
    building_type,
    floorarea_type,
    status as publish_flag,
    bedrooms,
    bathrooms,
    property_type,
    agent_id,
    price,
    price_qualifier,
    ingested_at,
    snapshot_date
from dedup