SELECT
    l.launch_id,
    l.mission_name,
    l.flight_number,
    l.launch_date_utc,
    l.is_success,
    l.is_upcoming,
    l.details,
    r.rocket_name,
    l.rocket_id,
    l.launchpad_id, 
    r.cost_per_launch,
    lp.launchpad_name,
    lp.region,
    lp.locality
FROM {{ ref('launches') }} l
LEFT JOIN {{ ref('rockets') }} r ON l.rocket_id = r.rocket_id
LEFT JOIN {{ ref('launchpads') }} lp ON l.launchpad_id = lp.launchpad_id