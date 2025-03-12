{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
)
select
    pickup_zone_id,
    count(*) as total_trips,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_percentage), 2) as avg_tip_pct,
    round(avg(trip_distance), 2) as avg_distance,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(trip_duration_min), 2) as avg_duration,
    count(distinct pickup_date) as active_days,
    round(count(*) * 1.0 / nullif(count(distinct pickup_date), 0), 1) as trips_per_day
from trips
group by 1
