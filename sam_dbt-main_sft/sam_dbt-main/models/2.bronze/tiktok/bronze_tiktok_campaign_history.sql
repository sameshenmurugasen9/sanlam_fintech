{{
    config(
        alias='campaign_history',
        materialized='table'
    )
}}

select 
    campaign_id,
    campaign_name,
    campaign_type,
    status as campaign_status,
    advertiser_id,
    -- Extract campaign metadata using naming conventions
    split(campaign_name, '_') as campaign_parts,
    case 
        when upper(replace(campaign_parts[3], '"', '')) like 'TIKTOK' then 3
        when upper(replace(campaign_parts[4], '"', '')) like 'TIKTOK' then 4
        else null
    end as platform_position,
    replace(campaign_parts[0], '"', '') as brand,
    replace(campaign_parts[1], '"', '') as business_unit,
    replace(campaign_parts[2], '"', '') as product,
    replace(campaign_parts[platform_position + 1], '"', '') as sub_campaign,
    replace(campaign_parts[platform_position + 2], '"', '') as phase,
    case 
        when regexp_like(lower(replace(campaign_parts[array_size(campaign_parts)-1], '"', '')), '^camp[0-9]{6}$')
        then lower(replace(campaign_parts[array_size(campaign_parts)-1], '"', ''))
        else null
    end as camp_ref
from {{ source('tiktok_ads', 'campaign_history') }}
