{{ config(
  materialized = 'view'
) }}

SELECT
  tx_id,
  code,
  memo,
  asset,
  amount_e8,
  from_addr,
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
    'midgard_failed_deposit_messages'
  ) }}
