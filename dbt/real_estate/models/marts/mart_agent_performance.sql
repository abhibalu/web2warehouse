{{ config(materialized='table', schema='marts') }}

with latest_status as (
  select
    ps.property_id,
    first_value(status) over (
      partition by ps.property_id 
      order by coalesce(valid_to, date '2999-12-31') desc
    ) as current_status
  from {{ ref('int_property_snapshot') }} ps
),
base as (
  select
    pb.property_id,
    pb.agent_id,
    pb.postcode
  from {{ ref('int_property_base') }} pb
  where pb.agent_id is not null
),
priced as (
  select
    ps.property_id,
    ps.price
  from {{ ref('int_property_snapshot') }} ps
)

select
  b.agent_id,
  count(distinct b.property_id) as properties_seen,
  count_if(ls.current_status in ('for_sale','active')) as active_listings,
  avg(p.price) as avg_price
from base b
left join latest_status ls on ls.property_id = b.property_id
left join priced p on p.property_id = b.property_id
group by 1


