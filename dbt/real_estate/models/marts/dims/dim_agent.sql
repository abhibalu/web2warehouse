{{ config(materialized='table', schema='marts') }}

with src as (
  select
    distinct agent_id
  from {{ ref('int_property_base') }}
  where agent_id is not null
)
select
  agent_id
from src


