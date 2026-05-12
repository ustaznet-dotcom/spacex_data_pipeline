{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(launchpad_id)'
) }}

SELECT
    assumeNotNull(launchpad_id) as launchpad_id,
    count() as total_launches,
    countIf(is_success = true) as successful_launches,
    round(countIf(is_success = true) / count() * 100, 2) as success_rate_pct,
    min(launch_date_utc) as first_launch_date,
    max(launch_date_utc) as last_launch_date
FROM postgresql(
    'postgres:5432',
    'spacex_raw',
    'launches',
    'admin',
    'admin',
    'silver'
)
WHERE is_upcoming = false
GROUP BY launchpad_id
ORDER BY launchpad_id