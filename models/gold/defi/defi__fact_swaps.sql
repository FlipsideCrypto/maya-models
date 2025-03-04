{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM, SWAPS' }} },
  unique_key = 'fact_swaps_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    block_timestamp,
    block_id,
    tx_id,
    blockchain,
    pool_name,
    from_address,
    native_to_address,
    to_pool_address,
    affiliate_address,
    affiliate_fee_basis_points,
    affiliate_addresses_array,
    affiliate_fee_basis_points_array,
    from_asset,
    to_asset,
    from_amount,
    to_amount,
    min_to_amount,
    from_amount_usd,
    to_amount_usd,
    cacao_usd,
    asset_usd,
    to_amount_min_usd,
    swap_slip_bp,
    liq_fee_cacao,
    liq_fee_cacao_usd,
    liq_fee_asset,
    liq_fee_asset_usd,
    streaming_count,
    streaming_quantity,
    _unique_key,
    _inserted_timestamp
  FROM
    {{ ref('silver__swaps') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a._unique_key']
  ) }} AS fact_swaps_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  blockchain,
  pool_name,
  from_address,
  native_to_address,
  to_pool_address,
  affiliate_address,
  affiliate_fee_basis_points,
  affiliate_addresses_array,
  affiliate_fee_basis_points_array,
  from_asset,
  to_asset,
  from_amount,
  to_amount,
  min_to_amount,
  from_amount_usd,
  to_amount_usd,
  cacao_usd,
  asset_usd,
  to_amount_min_usd,
  swap_slip_bp,
  liq_fee_cacao,
  liq_fee_cacao_usd,
  liq_fee_asset,
  liq_fee_asset_usd,
  streaming_count,
  streaming_quantity,
  A._inserted_timestamp,
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
