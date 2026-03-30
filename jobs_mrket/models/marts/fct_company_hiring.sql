with jobs as (
    select * from {{ ref('stg_jobs') }}
)

select
    company_name,
    count(*)                            as total_jobs,
    count(distinct city_normalized)     as cities_hiring_in,
    round(
        sum(case when salary_disclosed then 1 else 0 end) * 100.0
        / count(*), 1
    )                                   as salary_transparency_pct,
    min(scraped_date)                   as first_seen,
    max(scraped_date)                   as last_seen
from jobs
group by company_name
having count(*) >= 2
order by total_jobs desc