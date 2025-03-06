{{ config(
  materialized = 'view'
) }}

SELECT
  amount_e8,
  asset,
  from_addr,
  to_addr,
  memo,
  tx_id,
  raw_log,
  code,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_send_messages'
  ) }}
