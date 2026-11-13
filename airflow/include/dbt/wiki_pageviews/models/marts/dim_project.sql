with domain_codes as (

    select distinct domain_code
    from {{ ref('stg_pageviews') }}

),

project_codes as (

    select * from {{ ref('wikimedia_project_codes') }}

),

languages as (

    select * from {{ ref('languages') }}

),

parsed as (

    select
        domain_code,
        split(domain_code, '\\.') as parts,
        size(split(domain_code, '\\.')) as n
    from domain_codes

),

classified as (

    select
        domain_code,
        parts,
        n,
        parts[0] as first_part,
        (
            n >= 2
            and parts[1] = 'm'
            and not (
                n = 2
                and parts[0] in ('commons', 'meta', 'incubator', 'species',
                                 'strategy', 'outreach', 'usability', 'quality')
            )
        ) as is_mobile
    from parsed

),

with_codes as (

    select
        domain_code,
        first_part,
        is_mobile,
        case
            when n = 1 then null
            when is_mobile then try_element_at(parts, 3)
            else try_element_at(parts, 2)
        end as code_suffix,
        case
            when first_part in ('commons', 'meta', 'incubator', 'species',
                                'strategy', 'outreach', 'usability', 'quality', 'www')
                then null
            else first_part
        end as language_code
    from classified

),

enriched as (

    select
        w.domain_code,
        w.first_part,
        w.language_code,
        coalesce(l.language_name, 'unknown') as language_name,
        case
            when w.code_suffix is null then 'wikipedia'
            else coalesce(pc.project_family, 'unknown')
        end as project_family,
        case when w.is_mobile then 'mobile' else 'desktop' end as platform
    from with_codes as w
    left join project_codes as pc
        on w.code_suffix = pc.code_suffix
    left join languages as l
        on w.language_code = l.language_code

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['domain_code']) }} as project_key,
        domain_code,
        language_code,
        language_name,
        project_family,
        platform,
        concat(first_part, '.', project_family) as site
    from enriched

)

select * from final