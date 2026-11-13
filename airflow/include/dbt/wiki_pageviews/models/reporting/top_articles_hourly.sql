{{
    config(
        materialized='incremental',
        incremental_strategy='replace_where',
        incremental_predicates=["date_key = " ~(var('process_date')|replace('-',''))]
    )

}}

with facts as (
    select * from {{ref('fact_pageviews_hourly')}}
    {% if is_incremental() %}
    where date_key =  {{(var('process_date')|replace('-',''))}}
    {% endif %}
),

pages as (
    select page_key, page_title from {{ref('dim_page')}}
    where site = 'pl.wikipedia' and is_article
),

joined as (
    select f.date_key, f.hour_key, p.page_title, sum(f.view_count) as total_views from facts f
    inner join pages p on f.page_key = p.page_key
    group by f.date_key, f.hour_key, p.page_title
),

ranked as (
    select 
        date_key, 
        hour_key, 
        page_title, 
        row_number() over(partition by date_key, hour_key order by total_views desc, page_title asc) as rank_in_hour,
        total_views
    from joined
),

final as (
    select * from ranked
    where rank_in_hour <=100
)

select * from final