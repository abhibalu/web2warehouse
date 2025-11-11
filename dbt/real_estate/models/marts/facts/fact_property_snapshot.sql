{{ config(materialized='table', schema='marts') }}

select
  ps.property_id,
  ps.valid_from,
  ps.valid_to,
  ps.price,
  ps.status,
  pb.postcode,
  pb.agent_id
from {{ ref('int_property_snapshot') }} ps
join {{ ref('int_property_base') }} pb
  on pb.property_id = ps.property_id


