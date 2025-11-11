{{ config(materialized='table', schema='marts') }}

with latest as (
  select
    pb.property_id,
    pb.slug,
    pb.display_address,
    pb.town,
    pb.postcode,
    pb.area,
    pb.building_type,
    pb.floorarea_type,
    pb.bedrooms,
    pb.bathrooms,
    pb.property_type,
    pb.agent_id,
    pb.latitude,
    pb.longitude,
    ls.current_status
  from {{ ref('int_property_base') }} pb
  left join (
    select
      ps.property_id,
      first_value(status) over (
        partition by ps.property_id 
        order by coalesce(valid_to, date '2999-12-31') desc
      ) as current_status
    from {{ ref('int_property_snapshot') }} ps
  ) ls on ls.property_id = pb.property_id
)

select * from latest


