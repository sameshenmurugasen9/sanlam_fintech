{{
    config(
        alias='ad_report_daily',
        materialized='table'
    )
}}

select 
    stat_time_day as date,
    ad_id,
    adgroup_id,
    campaign_id,
    spend,
    reach,
    impressions,
    follows,
    comments,
    likes,
    profile_visits,
    shares,
    follows + comments + likes + profile_visits + shares + clicks as engagements,
    video_views_p_100,
    video_views_p_25,
    video_views_p_50,
    video_views_p_75,
    video_watched_2_s,
    video_watched_6_s,
    video_play_actions as video_views,
    clicks,
    total_landing_page_view as landing_page_views,
    conversion as conversions
from {{ source('tiktok', 'ad_report_daily') }}
