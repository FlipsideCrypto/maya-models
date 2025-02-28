{{ config(
  materialized = 'view'
) }}

SELECT
  key,
  VALUE,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_constants'
  ) }}
