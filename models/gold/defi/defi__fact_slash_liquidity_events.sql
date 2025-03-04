{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM, SWAPS' }} },
  unique_key = 'fact_slash_liquidity_events_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    bond_address,
    lp_address,
    asset,
    lp_units,
    asset_e8_loss,
    cacao_e10_loss,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__slash_liquidity_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id', 'a.bond_address', 'a.lp_address', 'a.asset']
  ) }} AS fact_slash_liquidity_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  bond_address,
  lp_address,
  asset,
  lp_units,
  asset_e8_loss,
  cacao_e10_loss,
  event_id,
  A._inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  base A
  JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_timestamp = b.timestamp

{% if is_incremental() %}
WHERE
  b.block_timestamp >= (
    SELECT
      MAX(
        block_timestamp - INTERVAL '1 HOUR'
      )
    FROM
      {{ this }}
  )
{% endif %}
