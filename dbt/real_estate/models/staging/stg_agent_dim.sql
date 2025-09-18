{{ config(materialized='view') }}

select *
from {{ source('lake', 'ext_agent_dim') }}
;