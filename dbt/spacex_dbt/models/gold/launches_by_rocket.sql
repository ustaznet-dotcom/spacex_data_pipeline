{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(rocket_id)'
) }}

SELECT
    assumeNotNull(rocket_id) AS rocket_id,
    assumeNotNull(rocket_name) AS rocket_name,
    count() AS total_launches,
    countIf(is_success = true) AS successful_launches,
    countIf(is_success = false) AS failed_launches,
    round(countIf(is_success = true) / count() * 100, 2) AS success_rate_pct,
    min(launch_date_utc) AS first_launch_date,
    max(launch_date_utc) AS last_launch_date
FROM postgresql(
    'postgres:5432',
    'spacex_raw',
    'launches_enriched',
    'admin',
    'admin',
    'silver'
)
WHERE is_upcoming = false
GROUP BY rocket_id, rocket_name
ORDER BY rocket_id