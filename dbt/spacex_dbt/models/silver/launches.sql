SELECT id as launch_id,
    payload->>'name' as mission_name,
    (payload->>'flight_number')::smallint as flight_number,
    (payload->>'date_utc')::timestamptz as launch_date_utc,
    (payload->>'success')::bool as is_success,
    (payload->>'upcoming')::bool as is_upcoming,
    payload->>'rocket' as rocket_id,
    payload->>'launchpad' as launchpad_id,
    payload->>'details' as details

FROM {{ source('bronze', 'launches') }}