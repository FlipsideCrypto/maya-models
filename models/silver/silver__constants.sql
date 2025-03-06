{{ config(
  materialized = 'view'
) }}

SELECT
  C.key,
  C.value
FROM
  {{ ref('bronze__constants') }} C
