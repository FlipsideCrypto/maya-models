{{ config(
  materialized = 'view'
) }}

SELECT
  tx_id,
  INTERVAL,
  quantity,
  COUNT,
  last_height,
  deposit_asset,
  deposit_e8,
  in_asset,
  in_e8,
  out_asset,
  out_e8,
  failed_swaps,
  failed_swap_reasons,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_streaming_swap_details_events'
  ) }}
