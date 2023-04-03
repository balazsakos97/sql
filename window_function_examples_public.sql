bi_logic_prep as (
    select 
        seed_people.row_id,
        seed_people.employee_email,
        lead(seed_people.start_date) over (partition by seed_people.employee_email order by seed_people.row_id asc)     as next_start_date,
        dateadd(hour, 23, dateadd(minute, 59,dateadd(day,-1,next_start_date)))                                          as next_start_date_minus_one,
        lag(seed_people.start_date) over (partition by seed_people.employee_email order by seed_people.row_id asc)      as prev_start_date,
        lag(seed_people.team_id) over (partition by seed_people.employee_email order by seed_people.row_id asc)         as prev_team_id,
        max(seed_people.row_id) over (partition by seed_people.employee_email)                                          as last_assignment_row,
        dates.last_order_assignment                                                                                     as last_assignment_date,
        coalesce(seed_people.team_id,prev_team_id)                                                                      as team_id,
        to_timestamp(coalesce(seed_people.start_date,prev_start_date)) as start_date,
        case when next_start_date_minus_one < seed_people.start_date then seed_people.start_date
            when seed_people.is_leaver = 'TRUE' and last_assignment_row = seed_people.row_id then dateadd(hour, 1, dates.last_order_assignment)
            when seed_people.is_leaver = 'TRUE' and last_assignment_row <> seed_people.row_id and next_start_date_minus_one >= seed_people.start_date then next_start_date_minus_one
            when seed_people.is_leaver = 'TRUE' and last_assignment_row <> seed_people.row_id and next_start_date_minus_one < seed_people.start_date then seed_people.start_date
            when last_assignment_row <> seed_people.row_id  then coalesce(next_start_date_minus_one,seed_people.start_date)
            else '2100-01-01' end as end_date,
        seed_people.is_leaver,
        seed_people.modified_at
    from {{ source('x','abc') }} seed_people
    left join date_prep dates
        on dates.email = seed_people.employee_email
)

select
    a.row_id,
    a.employee_email,
    b.team_type,
    b.team_name,
    b.team_office,
    b.team_country_md,
    a.start_date,
    to_timestamp_ntz(a.end_date) as end_date,
    a.is_leaver,
    a.modified_at
from bi_logic_prep as a
left join {{ source('x','abc') }} as b
    on a.team_id = b.team_id
where a.start_date <> a.end_date