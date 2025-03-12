{{ config(materialized='table', partition_by={"field": "pickup_date", "data_type": "date", "granularity": "month"}) }}

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
)
select
    pickup_date,
    time_of_day,
    is_weekend,
    count(*) as trip_count,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_percentage), 2) as avg_tip_pct,
    round(avg(trip_duration_min), 2) as avg_duration_min,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(avg_speed_mph), 2) as avg_speed
from trips
group by 1, 2, 3
