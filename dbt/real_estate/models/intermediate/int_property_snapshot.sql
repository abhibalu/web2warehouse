{{ config(
    materialized = 'view', 
    schema = 'int') 
    
    }}


with s as (
    select
        property_id,
        price,
        price_qualifier,
        status,
        dbt_valid_from::date as valid_from,
        dbt_valid_to::date   as valid_to
    from {{ ref('property_pricing_status') }}
)

select * from s


