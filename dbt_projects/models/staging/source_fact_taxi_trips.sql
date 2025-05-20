-- models/staging/source_fact_taxi_trips.sql
{{ config(materialized='view') }}

SELECT * FROM {{ source('postgres', 'fact_taxi_trips') }}