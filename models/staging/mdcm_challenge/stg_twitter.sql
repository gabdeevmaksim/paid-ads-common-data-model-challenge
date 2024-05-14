with twitter_data as (

select * from {{ ref('src_promoted_tweets_twitter_all_data') }}

)

select
  '' as ad_id,
  0 as add_to_cart,
  '' as adset_id,
  CAST(campaign_id as STRING) as campaign_id,
  channel,
  sum(clicks) as clicks,
  sum(comments) as comments,
  '' as creative_id,
  date,
  sum(engagements) as engagements,
  sum(impressions) as impressions,
  0 as installs,
  sum(likes) as likes,
  sum(url_clicks) as link_cliks,
  '' as placement_id,
  0 as post_click_conversions,
  0 as post_view_conversions,
  count(1) as posts,
  0 as purchase,
  0 as registrations,
  0 as revenue,
  sum(retweets) as shares,
  sum(spend) as spend,
  sum(video_total_views) as video_views
from twitter_data
group by ad_id, adset_id, campaign_id, creative_id, placement_id, channel, date
order by engagements desc