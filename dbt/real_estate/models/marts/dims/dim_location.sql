{{ config(materialized='table', schema='marts') }}

with src as (
  select distinct
    postcode,
    any_value(town) as town,
    any_value(area) as area
  from {{ ref('int_property_base') }}
  where postcode is not null
)
select * from src


