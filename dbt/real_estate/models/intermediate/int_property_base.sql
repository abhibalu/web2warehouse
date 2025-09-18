{{ config(
    materialized = 'view', 
    schema = 'int') 
    
    }}


with source as (
    select * from {{ ref('stg_property_flat')}}
),
dedup as(
    select *
    from source
    qualify row_number() over(
    partition by "result.pageContext.propertyData._id","result.pageContext.propertyData.crm_id",path
    order by ingested_at desc
    ) = 1
)
select 
"result.pageContext.propertyData._id" as prop_id,
path as path,
"result.pageContext.propertyData.crm_id" as crm_id,
"result.pageContext.propertyData.latitude" as latitude,
"result.pageContext.propertyData.longitude" as longitude,
"result.pageContext.propertyData.slug" as slug,
"result.pageContext.display_address" as address,
TRIM(SPLIT_PART("result.pageContext.display_address", ',', 2)) as town,
TRIM(split_part("result.pageContext.display_address", ',', array_length(str_split("result.pageContext.display_address", ',')))) AS postcode,
"result.pageContext.propertyData.area" as area,
"result.pageContext.propertyData.building.0" as building_type,
"result.pageContext.propertyData.floorarea_type" as floorarea_type,
"result.pageContext.propertyData.status" as publish_flag,
"result.pageContext.propertyData.extras.created_on"::date as created_date,
"result.pageContext.propertyData.bedroom" as bedrooms,
"result.pageContext.propertyData.bathroom" as bathrooms,
"result.pageContext.propertyData.reception" as property_type,
"result.pageContext.propertyData.listingHistory.agent" as agent_id
from dedup 




