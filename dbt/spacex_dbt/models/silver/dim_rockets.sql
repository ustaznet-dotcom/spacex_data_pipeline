SELECT
    rocket_id,
    rocket_name,
    country,
    cost_per_launch,
    is_active,
    first_flight_date,
    success_rate_pct
FROM {{ ref('rockets') }}