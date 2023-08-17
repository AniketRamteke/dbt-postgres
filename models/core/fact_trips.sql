{{ config(materialized="table") }}

with green_data as (
    select tripid, vendorid, ratecodeid, pulocationid, dolocationid,
    payment_type, payment_type_description,
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),

yellow_data as (
    select tripid, vendorid, ratecodeid, pulocationid, dolocationid,
    payment_type, payment_type_description,
        'Yellow' as service_type
    from {{ ref("stg_yellow_tripdata") }}
),

trips_union as (
    select * from green_data
    union all
    select * from yellow_data
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    trips_union.tripid,
    trips_union.vendorid,
    trips_union.service_type,
    trips_union.ratecodeid,
    trips_union.dolocationid,
    trips_union.pulocationid,
    pickup_zone.borough as pickup_borough,
    trips_union.payment_type,
    trips_union.payment_type_description
from trips_union
inner join dim_zones as pickup_zone
on trips_union.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_union.dolocationid = pickup_zone.locationid
    
