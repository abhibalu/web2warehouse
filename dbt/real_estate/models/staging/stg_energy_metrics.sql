{{ config(materialized='view') }}

select *
from {{ source('lake', 'ext_energy_metrics') }}