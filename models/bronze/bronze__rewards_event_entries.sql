{{ config(
  materialized = 'view'
) }}

SELECT
  pool,
  cacao_e8,
  saver_e8,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_rewards_event_entries'
  ) }}
