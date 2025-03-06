{{ config(
  materialized = 'view'
) }}

SELECT
  pool AS pool_name,
  asset_tx AS asset_tx_id,
  asset_chain AS asset_blockchain,
  asset_addr AS asset_address,
  asset_e8,
  cacao_tx AS cacao_tx_ID,
  cacao_addr AS cacao_address,
  cacao_e8,
  pending_type,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__pending_liquidity_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, asset_addr, cacao_addr
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
