-- models/marts/trip_summary_per_hour.sql
{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('hour', tpep_pickup_datetime) AS hour,
    weather_category,
    COUNT(*) AS trip_count,
    AVG(trip_duration) AS avg_trip_duration,
    AVG(tip_percentage) AS avg_tip_percentage
FROM {{ ref('trip_enriched') }}
GROUP BY 1, 2
ORDER BY 1, 2