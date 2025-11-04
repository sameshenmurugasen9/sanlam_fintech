{{
    config(
        alias='advertiser',
        materialized='table'
    )
}}

select 
    id as advertiser_id,
    name as account_name
from {{ source('tiktok', 'advertiser') }}
