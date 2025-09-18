{{ config(materialized='table', schema='marts') }}

with property_snapshot as (
    select * from {{ ref('int_property_snapshot') }}
),
property_base as (
    select
        prop_id as property_id,
        postcode
    from {{ ref('int_property_base') }}
)

select
    pb.postcode,
    avg(ps.price) as average_price
from property_snapshot ps
left join property_base pb on ps.property_id = pb.property_id
group by 1