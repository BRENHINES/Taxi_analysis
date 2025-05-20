-- models/intermediate/trip_enriched.sql
{{ config(materialized='table') }}

SELECT
    t.*,
    w.temperature,
    w.humidity,
    w.wind_speed,
    w.weather_category
FROM {{ ref('source_fact_taxi_trips') }} t
LEFT JOIN {{ ref('source_dim_weather') }} w
    ON DATE_TRUNC('hour', t.tpep_pickup_datetime) = DATE_TRUNC('hour', w.timestamp)