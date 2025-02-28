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
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__slash_liquidity_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id, bond_address, lp_address, asset
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
