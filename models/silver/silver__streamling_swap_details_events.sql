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
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__streamling_swap_details_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
