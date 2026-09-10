SELECT
    launchpad_id,
    launchpad_name,
    full_name,
    locality,
    region,
    latitude,
    longitude,
    status
FROM {{ ref('launchpads') }}
