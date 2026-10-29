
with fact as(
select sum(view_count) as total_views
from {{ref('fact_pageviews_hourly')}}


),

staging as (
    select sum(view_count) as total_views
    from {{ref('stg_pageviews')}}
)

select fact.total_views as fact_views,
        staging.total_views as staging_views
from fact
cross join staging 
where fact.total_views <> staging.total_views