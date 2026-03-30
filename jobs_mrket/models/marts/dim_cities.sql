with jobs as (
    select * from {{ ref('stg_jobs') }}
)

select
    city_normalized                             as city,
    count(*)                                    as total_jobs,
    count(distinct company_name)                as unique_companies,
    round(
        sum(case when experience_band = 'Fresher'
            then 1 else 0 end) * 100.0
        / count(*), 1
    )                                           as fresher_jobs_pct,
    round(
        sum(case when salary_disclosed
            then 1 else 0 end) * 100.0
        / count(*), 1
    )                                           as salary_disclosed_pct
from jobs
group by city_normalized
order by total_jobs desc