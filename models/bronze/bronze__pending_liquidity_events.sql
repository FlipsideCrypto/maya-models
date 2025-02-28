{{ config(
  materialized = 'view'
) }}

SELECT
  pool,
  asset_tx,
  asset_chain,
  asset_addr,
  asset_e8,
  cacao_tx,
  cacao_addr,
  cacao_e8,
  pending_type,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_pending_liquidity_events'
  ) }}
