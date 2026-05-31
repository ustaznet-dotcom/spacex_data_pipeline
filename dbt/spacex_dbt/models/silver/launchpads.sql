SELECT id as launchpad_id,
    payload->>'name' as launchpad_name,
    payload->>'full_name' as full_name,
    payload->>'locality' as locality,
    payload->>'region' as region,
    (payload->>'latitude')::numeric as latitude,
    (payload->>'longitude')::numeric as longitude,
    payload->>'status' as status

FROM {{ source('bronze', 'launchpads') }}
