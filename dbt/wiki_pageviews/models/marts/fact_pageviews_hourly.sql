with pageviews as (
    select * from {{ref('stg_pageviews') }}
),

projects as (
    select domain_code, site 
    from {{ref('dim_project')}}

),

joined as (
    select p.*, pr.site from pageviews p
    left join projects pr on p.domain_code = pr.domain_code
),

final as (
    select
        cast(date_format(view_date,'yyyyMMdd') as int) as date_key,
        hour(view_hour_utc) as hour_key,
        {{dbt_utils.generate_surrogate_key(["domain_code"])}} as project_key,
        {{dbt_utils.generate_surrogate_key(["site","page_title"])}} as page_key,
        view_count
    from joined

)

select * from final
