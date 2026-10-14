-- Every hour between the first and last row of silver_smard_hourly must be present.
-- Returns the missing hours; the test fails if any are found.
with bounds as (
    select min(timestamp_utc) as first_hour, max(timestamp_utc) as last_hour
    from {{ ref('silver_smard_hourly') }}
),

expected as (
    select unnest(generate_series(first_hour, last_hour, interval 1 hour)) as timestamp_utc
    from bounds
)

select e.timestamp_utc
from expected as e
left join {{ ref('silver_smard_hourly') }} as s using (timestamp_utc)
where s.timestamp_utc is null
