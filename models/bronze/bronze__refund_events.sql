{{ config(
  materialized = 'view'
) }}

SELECT
  tx,
  chain,
  from_addr,
  to_addr,
  asset,
  asset_e8,
  asset_2nd,
  asset_2nd_e8,
  memo,
  code,
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
    'midgard_refund_events'
  ) }}
