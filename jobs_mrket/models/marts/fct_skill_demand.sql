with jobs as (
    select skills from {{ ref('stg_jobs') }}
    where skills is not null
),

split_skills as (
    select
        trim(skill)                    as skill_name
    from jobs,
    unnest(string_to_array(skills, ',')) as skill
    where trim(skill) != ''
),

skill_counts as (
    select
        lower(trim(skill_name))        as skill_name,
        count(*)                       as job_count
    from split_skills
    group by lower(trim(skill_name))
)

select
    skill_name,
    job_count,
    round(job_count * 100.0 / sum(job_count) over(), 2) as demand_pct
from skill_counts
where job_count >= 2
order by job_count desc