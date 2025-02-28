{{ config(
  materialized = 'view'
) }}

SELECT
  from_addr,
  to_addr,
  asset,
  amount_e8,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_transfer_events'
  ) }}
