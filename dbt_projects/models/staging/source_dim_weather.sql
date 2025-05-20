-- models/staging/source_dim_weather.sql
{{ config(materialized='view') }}

SELECT * FROM {{ source('postgres', 'dim_weather') }}