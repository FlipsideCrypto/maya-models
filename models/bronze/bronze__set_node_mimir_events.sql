{{ config(
  materialized = 'view'
) }}

SELECT
  address,
  key,
  VALUE,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_set_node_mimir_events'
  ) }}
