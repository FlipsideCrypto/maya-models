{{ config(
    materialized = 'view'
) }}

SELECT
    '2.10.0' AS midgard_version
