{{ config(
  materialized = 'view'
) }}

SELECT
  amount_e8,
  asset,
  from_addr AS from_address,
  to_addr AS to_address,
  memo,
  tx_id,
  raw_log,
  code,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__send_messages') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
