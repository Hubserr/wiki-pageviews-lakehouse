with pageviews as (

    select domain_code, page_title
    from {{ ref('stg_pageviews') }}

),

projects as (

    select domain_code, site
    from {{ ref('dim_project') }}

),

namespaces as (

    select * from {{ ref('namespaces') }}

),

pages as (

    select distinct
        pr.site,
        pv.page_title
    from pageviews as pv
    left join projects as pr   
        on pv.domain_code = pr.domain_code

),

with_prefix as (

    select
        site,
        page_title,
        regexp_extract(page_title, '^([^:]+):', 1) as prefix
    from pages

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['p.site', 'p.page_title']) }} as page_key,
        p.site,
        p.page_title,
        n.namespace,
        n.namespace is null as is_article
    from with_prefix as p
    left join namespaces as n
        on p.prefix = n.prefix

)

select * from final