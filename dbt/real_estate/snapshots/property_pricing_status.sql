{% snapshot property_pricing_status %}
{{
  config(
    target_schema='snapshots',
    unique_key='property_id',
    strategy='check',
    check_cols=['price','status','price_qualifier']
  )
}}

with src as (
  select
    "result.pageContext.propertyData._id"::varchar as property_id,
    "result.pageContext.propertyData.price"::double as price,
    "result.pageContext.propertyData.price_qualifier"::varchar as price_qualifier,
    "result.pageContext.propertyData.status"::varchar as status,
    cast('{{ run_started_at }}' as date) as snapshot_date
  from {{ source('lake', 'ext_property_flat') }}
)
select * from src
{% endsnapshot %}


