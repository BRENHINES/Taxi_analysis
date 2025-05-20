-- models/marts/high_value_customers.sql
{{ config(materialized='table') }}

WITH customer_stats AS (
    SELECT
        passenger_count,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_spent,
        AVG(tip_percentage) AS avg_tip_percentage
    FROM {{ ref('trip_enriched') }}
    WHERE passenger_count > 0
    GROUP BY 1
)

SELECT *
FROM customer_stats
WHERE trip_count > 10
AND total_spent > 300
AND avg_tip_percentage > 15