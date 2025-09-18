
{{ config(materialized='view') }}



select *
from {{ source('lake', 'ext_image_items') }}

