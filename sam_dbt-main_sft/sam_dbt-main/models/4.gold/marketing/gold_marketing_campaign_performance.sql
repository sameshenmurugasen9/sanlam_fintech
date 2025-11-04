{{
    config(
        alias='campaign_performance'
        materialized='table' 
    )
}} -- Marts are heavily queried by BI tools; tables offer better query performance than views.

-- This model creates the final, wide, de-normalized table for the marketing team.
-- It joins all conformed data sources into one flat table for easy analysis.

with unioned_performance_data as (

    -- Import the conformed, cross-platform performance data from the Silver layer.
    -- This model is the single source of truth for all platform stats.
    select * from {{ ref('silver_marketing_campaign_stats') }}

)
-- You could join your 'silver_ga4_...' model here to add web metrics
-- with web_analytics as (
--     select * from {{ ref('silver_ga4_web_stats') }}
-- )

-- Final select statement to build the wide, flat table.
select
    
    -- DIMENSIONS
    -- These are the primary attributes for grouping and filtering.
    report_date,
    platform,
    campaign_id,
    campaign_name,

    -- CORE METRICS
    -- These are the base facts, directly from the source platforms.
    spend,
    impressions,
    clicks,
    conversions,
    
    -- CALCULATED METRICS
    -- Pre-calculate key ratios here to ensure consistent business logic
    -- across all reports and dashboards.
    -- We use 'safe_divide' (from dbt_utils) to prevent 'divide by zero' errors.
    
    -- ctr: Click-Through Rate (Ad engagement)
    safe_divide(clicks, impressions) as ctr,
    
    -- cpc: Cost Per Click (Cost efficiency)
    safe_divide(spend, clicks) as cpc,
    
    -- cpa: Cost Per Acquisition (Conversion cost)
    safe_divide(spend, conversions) as cpa,
    
    -- conversion_rate: Click-to-Conversion Rate (Ad effectiveness)
    safe_divide(conversions, clicks) as conversion_rate

from unioned_performance_data

-- This model is intentionally de-normalized (flat) for ease of use in
-- BI tools (e.g., Looker, Tableau) and for ML feature engineering.
-- All complex joins are handled upstream in the Silver layer.
