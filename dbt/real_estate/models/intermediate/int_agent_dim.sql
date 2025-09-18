{{ config(materialized = 'view', schema = 'int') }}

select distinct
    agent_id,           
    agent_name,
    agent_email
from {{ref('stg_agent_dim')}} 
