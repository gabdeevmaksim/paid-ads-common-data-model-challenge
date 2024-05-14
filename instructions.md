## How to add data from a new ad platform

This guide outlines the steps to incorporate data from a new ad platform into existing data pipeline.

**1. Uploading the Data**

* Begin by uploading the new table containing ad performance data to your database.

**2. Creating the Staging SQL File**

1. Create a new SQL file named `stg_<new_platform>.sql`.
2. Copy the following code template into the file:

```sql
with <new_platform>_data as (

  select * from {{ ref('<name of the new table>') }}

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
from <platform>_data
group by ad_id, adset_id, campaign_id, creative_id, placement_id, channel, date
order by date
```

**3. Data Mapping and Transformation**

Before running this code, carefully examine the column names in your new source table. Ensure they are mapped and data types are converted to match the template's expectations from `mcdm_paid_ads_basic_performance_structure.csv`.

**Examples:**

* TikTok source table: Replace `adset_id` with `adgroup_id` (if applicable).
* Facebook source table: Replace `revenue` with `purchase_value` (if applicable).

You might also need to calculate specific metrics:

* Facebook's `engagement` metric requires combining `clicks` and `views`.

If you don not have a suitable value to fill the column use `''` for string and `0` for numerical columns.

*  '' as placement_id,
*  0 as post_view_conversions,

**4. Adding the New Platform to the Main Model**

1. Open the `mdcm_table.sql` model file.
2. In the upper section, add a new Common Table Expression (CTE) named appropriately for your platform:

```sql
<name for CTE> as (

  select * from {{ ref('stg_<new_platform>') }}

)
```

3. In the lower section, incorporate the new platform data using a `UNION ALL` statement:

```sql
union all
select *, post_click_conversions+post_view_conversions as total_conversions
  from <new_platform>
```

**5. Materializing the Updated Table**

Use the `dbt run` command to execute the updated model and materialize the new table with the integrated data from your new ad platform.
