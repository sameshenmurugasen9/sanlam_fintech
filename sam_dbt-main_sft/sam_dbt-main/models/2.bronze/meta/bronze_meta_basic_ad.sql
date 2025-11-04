{{
    config(
        alias='basic_ad',
        materialized='table'
    )
}}

select 
    date,
    ad_id,
    campaign_id,
    spend,
    reach,
    impressions
from {{ source('meta', 'basic_ad') }}
