with facebook_data as (

select * from {{ ref('src_ads_creative_facebook_all_data') }}

)

select 
  CAST(ad_id AS STRING) as ad_id,
  sum(add_to_cart) as add_to_cart,
  CAST(adset_id AS STRING) as adset_id,
  CAST(campaign_id AS STRING) as campaign_id,
  channel,
  sum(clicks) as clicks,
  sum(comments) as comments,
  CAST(creative_id as STRING) as creative_id, 
  date,
  sum(clicks+views) as engagements,
  sum(impressions) as impressions,
  sum(mobile_app_install) as installs,
  sum(likes) as likes,
  sum(inline_link_clicks) as link_cliks,
  '' as placement_id,
  sum(purchase) as post_click_conversions,
  0 as post_view_conversions,
  count(1) as posts,
  sum(purchase) as purchase,
  sum(complete_registration) as registrations,
  sum(purchase_value) as revenue,
  sum(shares) as shares,
  sum(spend) as spend,
  sum(views) as video_views
from facebook_data
group by ad_id, adset_id, campaign_id, creative_id, placement_id, channel, date
order by date