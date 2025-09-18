{{ config(
    materialized = 'view', 
    schema = 'int') 
    
    }}


with src as (
    select * from {{ ref('stg_property_flat')}}
),
changes as(
    select 
    "result.pageContext.propertyData._id" as property_id,
    snapshot_date ::date as effective_date,
    "result.pageContext.propertyData.price" as price,
    "result.pageContext.propertyData.price_qualifier" as price_qualifier,
    "result.pageContext.propertyData.status" as status,
    row_number() over (
            partition by property_id, price, status
            order by snapshot_date
        ) as rn
    from src
),
scd2 as (
select
        property_id,
        price,
        price_qualifier,
        status,
        effective_date as valid_from,
        lead(effective_date) over (
            partition by property_id
            order by effective_date
        ) - interval 1 day as valid_to
    from changes
    where rn = 1 
)

select * from scd2


