{{ config(materialized='table', schema='marts') }}

with property_snapshot as (
    select * from {{ ref('int_property_snapshot') }}
),
property_base as (
    select
        property_id,
        postcode,
        bedrooms
    from {{ ref('int_property_base') }}
),
latest_status as (
    select
        ps.property_id,
        first_value(status) over (
            partition by ps.property_id 
            order by coalesce(valid_to, date '2999-12-31') desc
        ) as current_status
    from property_snapshot ps
),
price_points as (
    select
        ps.property_id,
        ps.price,
        ps.valid_from,
        ps.valid_to
    from property_snapshot ps
)

select
    pb.postcode,
    count(distinct pb.property_id) as properties_seen,
    count_if(ls.current_status in ('for_sale','active')) as active_listings,
    avg(pp.price) as avg_price,
    median(pp.price) as median_price,
    min(pp.price) as min_price,
    approx_quantile(pp.price, 0.95) as p95_price
from price_points pp
join property_base pb on pp.property_id = pb.property_id
left join latest_status ls on ls.property_id = pb.property_id
group by 1