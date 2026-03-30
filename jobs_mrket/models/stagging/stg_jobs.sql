with source as (
    select * from {{ source('raw', 'raw_jobs') }}
),

cleaned as (
    select
        id,
        lower(trim(job_title))                        as job_title,
        trim(company_name)                            as company_name,

        trim(location)                                as location,
        case
            when location ilike '%bangalore%'
              or location ilike '%bengaluru%'  then 'Bangalore'
            when location ilike '%mumbai%'     then 'Mumbai'
            when location ilike '%delhi%'
              or location ilike '%noida%'
              or location ilike '%gurgaon%'    then 'Delhi NCR'
            when location ilike '%hyderabad%'  then 'Hyderabad'
            when location ilike '%pune%'       then 'Pune'
            when location ilike '%chennai%'    then 'Chennai'
            when location ilike '%indore%'     then 'Indore'
            when location ilike '%remote%'     then 'Remote'
            else 'Other'
        end                                           as city_normalized,

        trim(experience)                              as experience,
        case
            when experience ilike '%0%'
              or experience ilike '%fresher%'   then 'Fresher'
            when experience ilike '%1%'
              or experience ilike '%2%'
              or experience ilike '%3%'         then '1-3 years'
            when experience ilike '%4%'
              or experience ilike '%5%'
              or experience ilike '%6%'         then '4-6 years'
            else '7+ years'
        end                                           as experience_band,

        trim(salary)                                  as salary,
        case
            when salary ilike '%not%'
              or salary is null               then false
            else true
        end                                           as salary_disclosed,

        trim(skills)                                  as skills,
        job_url,
        source,
        scraped_at::date                              as scraped_date

    from source
    where job_title is not null
      and company_name is not null
)

select * from cleaned