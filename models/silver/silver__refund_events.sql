{{ config(
  materialized = 'view'
) }}

SELECT
  tx AS tx_id,
  chain AS blockchain,
  from_addr AS from_address,
  to_addr AS to_address,
  asset,
  asset_e8,
  asset_2nd,
  asset_2nd_e8,
  memo,
  code,
  reason,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__refund_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
