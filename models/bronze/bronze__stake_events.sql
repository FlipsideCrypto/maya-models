{{ config(
  materialized = 'view'
) }}

SELECT
  pool,
  asset_tx,
  asset_chain,
  asset_addr,
  asset_e8,
  stake_units,
  cacao_tx,
  cacao_addr,
  cacao_e8,
  _ASSET_IN_CACAO_E8,
  memo,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_stake_events'
  ) }}
