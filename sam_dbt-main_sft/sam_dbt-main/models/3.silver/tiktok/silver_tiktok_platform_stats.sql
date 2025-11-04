{{
    config(
        alias='platform_stats',
        materialized='table'
    )
}}

select 
    to_date(t.date) as date,
    to_char(to_date(t.date), 'YYYYMMDD') as date_id,
    concat('tiktok_campaign_', cast(t.campaign_id as varchar)) as campaign_id,
    'tiktok' as channel,
    tc.campaign_name,
    tc.campaign_status,
    tc.brand,
    tc.business_unit,
    tc.product,
    tc.sub_campaign,
    tc.phase,
    tc.camp_ref,
    ta.account_name,
    sum(t.spend) as spend,
    sum(t.impressions) as impressions,
    sum(t.reach) as reach,
    sum(t.follows) as follows,
    sum(t.comments) as comments,
    sum(t.likes) as likes,
    sum(t.profile_visits) as profile_visits,
    sum(t.shares) as shares,
    cast(null as int) as posts,
    cast(null as int) as page_engagements,
    cast(null as int) as post_engagements,
    sum(t.engagements) as engagements,
    sum(t.video_views_p_25) as video_views_p_25,
    sum(t.video_views_p_50) as video_views_p_50,
    sum(t.video_views_p_75) as video_views_p_75,
    sum(t.video_views_p_100) as video_views_p_100,
    sum(t.video_watched_2_s) as video_watched_2_s,
    sum(t.video_watched_6_s) as video_watched_6_s,
    sum(t.video_views) as video_views,
    sum(t.clicks) as clicks,
    sum(t.landing_page_views) as landing_page_views,
    sum(t.conversions) as conversions
from {{ ref('bronze_tiktok_ad_report_daily') }} t

left join {{ ref('bronze_tiktok_campaign_history') }} tc 
    on t.campaign_id = tc.campaign_id

left join {{ ref('bronze_tiktok_advertiser') }} ta 
    on tc.advertiser_id = ta.advertiser_id

where tc._fivetran_active = true
group by all
