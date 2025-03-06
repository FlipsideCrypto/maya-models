{{ config(
  materialized = 'view'
) }}

SELECT
  node_address,
  slash_points,
  reason,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_slash_points_events'
  ) }}
