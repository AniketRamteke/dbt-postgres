{{ config(materialized='view') }}

select *,
        {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime'])}} as tripid, 
        cast(payment_type as numeric) as payment_type_info,
        {{ get_payment_type_description('payment_type')}} as payment_type_description
    from {{ source('staging', 'green_tripdata') }}
{% if var('is_test_run') %}
    limit 10000
{% endif %}
