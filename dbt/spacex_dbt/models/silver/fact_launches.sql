SELECT
    launch_id,
    rocket_id,
    launchpad_id,
    is_success,
    flight_number,
    launch_date_utc,
    mission_name,
    details
FROM {{ ref('launches') }}

