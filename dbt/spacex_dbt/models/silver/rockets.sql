SELECT id as rocket_id,
    payload->>'name' as rocket_name,
    payload->>'country' as country,
    (payload->>'cost_per_launch')::integer as cost_per_launch,
    (payload->>'first_flight')::date as first_flight_date,
    (payload->>'active')::bool as is_active,
    (payload->>'success_rate_pct')::smallint as success_rate_pct

FROM {{ source('bronze', 'rockets') }}

