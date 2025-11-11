{{  
    config(
        materialized = 'view',
        schema       = 'staging'  
    ) 
}}

with src as (
    select 
        "result.pageContext.propertyData._id"::varchar as property_id,
        "result.pageContext.propertyData.crm_id"::varchar as crm_id,
        "result.pageContext.propertyData.latitude"::double as latitude,
        "result.pageContext.propertyData.longitude"::double as longitude,
        "result.pageContext.propertyData.slug"::varchar as slug,
        "result.pageContext.display_address"::varchar as display_address,
        "result.pageContext.propertyData.address.address2"::varchar as town,
        "result.pageContext.propertyData.address.postcode"::varchar as postcode,
        "result.pageContext.propertyData.area"::varchar as area,
        "result.pageContext.propertyData.building.0"::varchar as building_type,
        "result.pageContext.propertyData.floorarea_type"::varchar as floorarea_type,
        "result.pageContext.propertyData.status"::varchar as status,
        "result.pageContext.propertyData.reception"::varchar as property_type,
        "result.pageContext.propertyData.bedroom"::int as bedrooms,
        "result.pageContext.propertyData.bathroom"::int as bathrooms,
        "result.pageContext.propertyData.listingHistory.agent"::varchar as agent_id,
        "result.pageContext.propertyData.price"::double as price,
        "result.pageContext.propertyData.price_qualifier"::varchar as price_qualifier,
        path::varchar as path,
        ingested_at::timestamp as ingested_at,
        cast('{{ run_started_at }}' as date) as snapshot_date
    from {{ source('lake', 'ext_property_flat') }}
)

select 
    *
from src