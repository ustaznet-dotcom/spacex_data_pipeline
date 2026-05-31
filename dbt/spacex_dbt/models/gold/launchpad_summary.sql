{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(launchpad_id)'
) }}

SELECT
    assumeNotNull(launchpad_id) as launchpad_id,
    assumeNotNull(launchpad_name) AS launchpad_name,
    assumeNotNull(region) AS region,
    count() as total_launches,
    countIf(is_success = true) as successful_launches,
    round(countIf(is_success = true) / count() * 100, 2) as success_rate_pct,
    min(launch_date_utc) as first_launch_date,
    max(launch_date_utc) as last_launch_date
FROM postgresql(
    'postgres:5432',
    'spacex_raw',
    'launches_enriched',
    'admin',
    'admin',
    'silver'
)
WHERE is_upcoming = false
GROUP BY launchpad_id, launchpad_name, region
ORDER BY launchpad_id