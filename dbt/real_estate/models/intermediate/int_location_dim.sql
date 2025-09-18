{{ config(materialized = 'view', schema = 'int') }}

with src as (

    select *
    from {{ ref('stg_location_dim') }}

)

select distinct
    address_line3 as county,
    address_line2 as town,
    postcode,
    -- surrogate key if you like
    {{ dbt_utils.generate_surrogate_key(['county','town','postcode']) }}
        as location_sk
from src
