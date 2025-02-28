{{ config(
  materialized = 'view'
) }}

SELECT
  tx_id,
  code,
  memo,
  asset,
  amount_e8,
  from_addr AS from_address,
  reason,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref(
    'bronze__failed_deposit_messages'
  ) }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
