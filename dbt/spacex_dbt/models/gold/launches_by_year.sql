{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(year)'
) }}

SELECT
    toYear(assumeNotNull(launch_date_utc)) AS year,
    count() AS total_launches,
    countIf(is_success = true) AS successful_launches,
    countIf(is_success = false) AS failed_launches,
    round(countIf(is_success = true) / count() * 100, 2) AS success_rate_pct
FROM postgresql(
    'postgres:5432',
    'spacex_raw',
    'launches_enriched',
    'admin',
    'admin',
    'silver'
)
WHERE is_upcoming = false
GROUP BY year
ORDER BY year