{{ config(materialized='table', schema='marts') }}

with src as (
  select
    r.property_id,
    r.room_name,
    r.room_type,
    r.snapshot_date
  from {{ ref('int_room_items') }} r
)
select * from src


