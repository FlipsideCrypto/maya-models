{{ config(
  materialized = 'view'
) }}

SELECT
  from_addr AS from_address,
  to_addr AS to_address,
  asset,
  amount_e8,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__transfer_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, from_addr, to_addr, asset, amount_e8
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
