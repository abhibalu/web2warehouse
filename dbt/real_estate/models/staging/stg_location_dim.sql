{{ config(materialized='view') }}

select *
from {{ source('lake', 'ext_location_dim') }}