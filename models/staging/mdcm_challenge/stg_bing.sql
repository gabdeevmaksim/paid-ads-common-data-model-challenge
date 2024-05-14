with bing_data as (

select * from {{ ref('src_ads_bing_all_data') }}

)

select 
  CAST(ad_id AS STRING) as ad_id,
  0 as add_to_cart,
  CAST(adset_id AS STRING) as adset_id,
  CAST(campaign_id AS STRING) as campaign_id,
  channel,
  sum(clicks) as clicks,
  0 as comments,
  '' as creative_id,
  date,
  0 as engagements,
  sum(imps) as impressions,
  0 as installs,
  0 as likes,
  0 as link_cliks,
  '' as placement_id,
  sum(conv) as post_click_conversions,
  0 as post_view_conversions,
  count(1) as posts,
  0 as purchase,
  0 as registrations,
  sum(revenue) as revenue,
  0 as shares,
  sum(spend) as spend,
  0 as video_views
  
from bing_data
group by ad_id, adset_id, campaign_id, creative_id, placement_id, channel, date
order by date asc