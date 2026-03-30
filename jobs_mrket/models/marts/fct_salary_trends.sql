with jobs as (
    select * from {{ ref('stg_jobs') }}
    where salary_disclosed = true
      and salary not ilike '%not%'
)

select
    experience_band,
    city_normalized,
    count(*)                as job_count,
    salary
from jobs
group by experience_band, city_normalized, salary
order by experience_band, job_count desc