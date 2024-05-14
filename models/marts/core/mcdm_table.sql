with bing as (

    select * from {{ ref('stg_bing') }}

),

facebook as (

    select * from {{ ref('stg_facebook') }}
),

tiktok as (

    select * from {{ ref('stg_tiktok') }}
),

twitter as (

    select * from {{ ref('stg_twitter') }}
)

select *, post_click_conversions+post_view_conversions as total_conversions
    from bing 
    union all
select *, post_click_conversions+post_view_conversions as total_conversions
    from facebook
    union all
select *, post_click_conversions+post_view_conversions as total_conversions
    from tiktok
    union all
select *, post_click_conversions+post_view_conversions as total_conversions
    from twitter
