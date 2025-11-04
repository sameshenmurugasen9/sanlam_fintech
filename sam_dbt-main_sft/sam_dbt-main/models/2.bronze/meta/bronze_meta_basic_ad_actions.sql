{{
    config(
        alias='basic_ad_actions',
        materialized='table'
    )
}}

select 
    ad_id,
    date,
    action_type
from {{ source('meta', 'basic_ad_actions') }}
