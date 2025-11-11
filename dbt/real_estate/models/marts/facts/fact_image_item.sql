{{ config(materialized='table', schema='marts') }}

with src as (
  select
    i.property_id,
    i.image_url,
    i.image_type,
    i.snapshot_date
  from {{ ref('stg_image_items') }} i
)
select * from src


