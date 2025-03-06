{{ config(
  materialized = 'view'
) }}

SELECT
  tx,
  chain,
  from_addr,
  to_addr,
  from_asset,
  from_e8,
  to_asset,
  to_e8,
  memo,
  pool,
  to_e8_min,
  swap_slip_bp,
  liq_fee_e8,
  liq_fee_in_cacao_e8,
  _DIRECTION,
  _STREAMING,
  streaming_count,
  streaming_quantity,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_swap_events'
  ) }}
