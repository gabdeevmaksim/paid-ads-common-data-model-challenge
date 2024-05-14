with tiktok_data as (

select * from {{ ref('src_ads_tiktok_ads_all_data') }}

)

select 
  CAST(ad_id AS STRING) as ad_id,
  sum(add_to_cart) as add_to_cart,
  CAST(adgroup_id AS STRING) as adset_id,
  CAST(campaign_id AS STRING) as campaign_id,
  channel,
  sum(clicks) as clicks,
  0 as comments,
  '' as creative_id,
  date,
  0 as engagements,
  sum(impressions) as impressions,
  sum(rt_installs + skan_app_install) as installs,
  0 as likes,
  0 as link_clicks,
  '' as placement_id,
  0 as post_click_conversions,
  sum(conversions) as post_view_conversions,
  count(1) as posts,
  sum(purchase) as purchase,
  sum(registrations) as registrations,
  0 as revenue,
  0 as shares,
  sum(spend) as spend,
  sum(video_views) as video_views
from tiktok_data
group by ad_id, adset_id, campaign_id, creative_id, placement_id, channel, date
order by date