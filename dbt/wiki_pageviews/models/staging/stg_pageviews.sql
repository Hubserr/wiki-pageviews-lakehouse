with source as (
    select * from {{ source('silver','pageviews')}}
),

renamed as (
    select
        domain_code,
        page_title,
        view_hour_utc,
        view_date,
        view_count
    from source
)

select * from renamed