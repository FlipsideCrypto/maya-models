{{ config(
  materialized = 'view'
) }}

SELECT
  pool AS pool_name,
  asset_tx AS asset_tx_id,
  asset_chain AS asset_blockchain,
  asset_addr AS asset_address,
  asset_e8,
  stake_units,
  cacao_tx AS cacao_tx_id,
  cacao_addr AS cacao_address,
  cacao_e8,
  _ASSET_IN_CACAO_E8,
  memo,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__stake_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, cacao_addr, asset_addr
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
