{{
    config(
        alias='platform_stats',
        materialized='table'
    )
}}

select 
    to_date(m.date) as date,
    to_char(to_date(m.date), 'YYYYMMDD') as date_id,
    concat('meta_campaign_', cast(m.campaign_id as varchar)) as campaign_id,
    'meta' as channel,
    mc.campaign_name,
    mc.campaign_status,
    mc.brand,
    mc.business_unit,
    mc.product,
    mc.sub_campaign,
    mc.phase,
    mc.camp_ref,
    ma.account_name,
    sum(m.spend) as spend,
    sum(m.impressions) as impressions,
    sum(m.reach) as reach,
    cast(null as int) as follows,
    sum(mact.comments) as comments,
    sum(mact.likes) as likes,
    cast(null as int) as profile_visits,
    cast(null as int) as shares,
    sum(mact.posts) as posts,
    sum(mact.page_engagements) as page_engagements,
    sum(mact.post_engagements + mact.post_reactions) as post_engagements,
    sum(mact.page_engagements + mact.post_engagements + mact.post_reactions + mact.posts + mact.comments + mact.likes) as engagements,
    cast(null as int) as video_views_p_25,
    cast(null as int) as video_views_p_50,
    cast(null as int) as video_views_p_75,
    cast(null as int) as video_views_p_100,
    cast(null as int) as video_watched_2_s,
    cast(null as int) as video_watched_6_s,
    sum(mact.video_views) as video_views,
    sum(mact.clicks) as clicks,
    sum(mact.landing_page_views) as landing_page_views,
    sum(mact.onsite_conversions + mact.offsite_conversions + mact.leads) as conversions
from {{ ref('bronze_meta_basic_ad') }} m

left join {{ ref('bronze_meta_basic_ad_actions') }} mact 
    on m.ad_id = mact.ad_id and m.date = mact.
    
left join {{ ref('bronze_meta_campaign_history') }} mc 
    on m.campaign_id = mc.campaign_id

left join {{ ref('bronze_meta_account_history') }} ma 
    on mc.advertiser_id = ma.advertiser_id

where mc._fivetran_active = true
    and ma._fivetran_active = true

group by all
