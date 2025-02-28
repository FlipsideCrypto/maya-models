{{ config(
  materialized = 'view'
) }}

SELECT
  bond_address,
  lp_address,
  asset,
  lp_units,
  asset_e8_loss,
  cacao_e10_loss,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_slash_liquidity_events'
  ) }}
