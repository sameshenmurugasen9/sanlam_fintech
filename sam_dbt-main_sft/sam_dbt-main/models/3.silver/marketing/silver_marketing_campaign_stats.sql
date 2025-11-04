{{
    config(
        alias='campaign_stats',
        materialized='table'
    )
}}

with all_platform_stats as (
    select *
    from {{ ref('silver_tiktok_platform_stats') }}

    union all

    select *
    from {{ ref('silver_meta_platform_stats') }}

    --union all

    --other platforms
),

-- Monthly Aggregation
monthly_campaign_performance as (
    select 
        ds.year_month,
        ds.week_number,
        aps.campaign_id,
        aps.channel,
        aps.campaign_name,
        aps.campaign_status,
        aps.brand,
        aps.business_unit,
        aps.product,
        aps.sub_campaign,
        aps.phase,
        aps.camp_ref,
        aps.account_name,
        -- Calculate campaign age
        datediff('day', min(aps.date) over (partition by aps.campaign_id), current_date()) as campaign_age,
        -- Aggregate metrics by month
        sum(aps.spend) as spend,
        sum(aps.impressions) as impressions,
        sum(aps.reach) as reach,
        case when sum(aps.reach) > 0 then sum(aps.impressions) / sum(aps.reach) end as frequency,
        sum(aps.follows) as follows,
        sum(aps.comments) as comments,
        sum(aps.likes) as likes,
        sum(aps.profile_visits) as profile_visits,
        sum(aps.shares) as shares,
        sum(aps.posts) as posts,
        sum(aps.page_engagements) as page_engagements,
        sum(aps.post_engagements) as post_engagements,
        sum(aps.engagements) as engagements,
        sum(aps.video_views_p_25) as video_views_p_25,
        sum(aps.video_views_p_50) as video_views_p_50,
        sum(aps.video_views_p_75) as video_views_p_75,
        sum(aps.video_views_p_100) as video_views_p_100,
        sum(aps.video_watched_2_s) as video_watched_2_s,
        sum(aps.video_watched_6_s) as video_watched_6_s,
        sum(aps.video_views) as video_views,
        sum(aps.clicks) as clicks,
        sum(aps.landing_page_views) as landing_page_views,
        sum(aps.conversions) as conversions,
        -- GA4 metrics
        sum(gp.page_views) as page_views,
        sum(gp.engaged_page_views) as engaged_page_views,
        sum(gp.users) as users,
        sum(gp.mins_engaged) as mins_engaged,
        sum(gl.call_me_back) as call_me_back,
        sum(gl.email_lead) as email_lead,
        sum(gl.coach_cellphone_submit) as coach_cellphone_submit,
        sum(gl.coach_email) as coach_email,
        sum(gl.coach_otp) as coach_otp,
        sum(gl.tfsa_online_application) as tfsa_online_application,
        sum(gl.ra_online_application) as ra_online_application
    from all_platform_stats aps

    left join {{ ref('date_dimension') }} ds 
        on aps.date_id = ds.date_id

    left join {{ ref('silver_ga4_pageviews') }} gp 
        on aps.date_id = gp.date_id 
        and aps.camp_ref = gp.utm_camp_ref

    left join {{ ref('silver_ga4_leads') }} gl 
        on aps.date_id = gl.date_id 
        and aps.camp_ref = gl.utm_camp_ref
    
    group by all
),

campaign_performance_monthly as (
    select 
        year_month,
        concat(year_month, '-W', week_number) as year_week,
        campaign_id,
        channel,
        campaign_name,
        campaign_status,
        brand,
        business_unit,
        product,
        sub_campaign,
        phase,
        camp_ref,
        account_name,
        campaign_age,
        spend,
        impressions,
        reach,
        frequency,
        follows,
        comments,
        likes,
        profile_visits,
        shares,
        posts,
        page_engagements,
        post_engagements,
        engagements,
        video_views_p_25,
        video_views_p_50,
        video_views_p_75,
        video_views_p_100,
        video_watched_2_s,
        video_watched_6_s,
        video_views,
        clicks,
        landing_page_views,
        conversions,
        page_views,
        engaged_page_views,
        users,
        mins_engaged,
        call_me_back,
        email_lead,
        coach_cellphone_submit,
        coach_email,
        coach_otp,
        tfsa_online_application,
        ra_online_application
from monthly_campaign_performance
)

select * from campaign_performance_monthly
