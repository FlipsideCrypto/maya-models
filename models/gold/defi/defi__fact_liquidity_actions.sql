{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_liquidity_actions_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    block_id,
    tx_id,
    lp_action,
    pool_name,
    from_address,
    to_address,
    cacao_amount,
    cacao_amount_usd,
    asset_amount,
    asset_amount_usd,
    stake_units,
    asset_tx_id,
    asset_address,
    asset_blockchain,
    il_protection,
    il_protection_usd,
    unstake_asymmetry,
    unstake_basis_points,
    _unique_key,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__liquidity_actions') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a._unique_key']
  ) }} AS fact_liquidity_actions_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  lp_action,
  pool_name,
  from_address,
  to_address,
  cacao_amount,
  cacao_amount_usd,
  asset_amount,
  asset_amount_usd,
  stake_units,
  asset_tx_id,
  asset_address,
  asset_blockchain,
  il_protection,
  il_protection_usd,
  unstake_asymmetry,
  unstake_basis_points,
  A._INSERTED_TIMESTAMP,
  '{{ invocation_id }}' AS _invocation_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  base A
  JOIN {{ ref('core__dim_block') }}
  b
  ON A.block_id = b.block_id

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
