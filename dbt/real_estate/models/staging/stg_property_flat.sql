
{{  
    config(
        materialized = 'view',
        schema       = 'staging'  
    ) 
}}


with src as (
    select 
        *
    from {{ source('lake', 'ext_property_flat') }}
)

select 
    src.*,
    cast('{{ run_started_at }}' as date) as snapshot_date
from src