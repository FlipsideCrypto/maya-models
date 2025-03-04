{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_daily_pool_stats_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.day >= (select min(day) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['day']
) }}

WITH base AS (

  SELECT
    DAY,
    pool_name,
    system_rewards,
    system_rewards_usd,
    asset_liquidity,
    asset_price,
    asset_price_usd,
    cacao_liquidity,
    cacao_price,
    cacao_price_usd,
    add_liquidity_count,
    add_asset_liquidity,
    add_asset_liquidity_usd,
    add_cacao_liquidity,
    add_cacao_liquidity_usd,
    withdraw_count,
    withdraw_asset_liquidity,
    withdraw_asset_liquidity_usd,
    withdraw_cacao_liquidity,
    withdraw_cacao_liquidity_usd,
    il_protection_paid,
    il_protection_paid_usd,
    average_slip,
    to_asset_average_slip,
    to_cacao_average_slip,
    swap_count,
    to_asset_swap_count,
    to_cacao_swap_count,
    swap_volume_cacao,
    swap_volume_cacao_usd,
    to_asset_swap_volume,
    to_cacao_swap_volume,
    total_swap_fees_cacao,
    total_swap_fees_usd,
    total_asset_swap_fees,
    total_asset_cacao_fees,
    unique_member_count,
    unique_swapper_count,
    liquidity_units,
    _UNIQUE_KEY
  FROM
    {{ ref('silver__daily_pool_stats') }}

{% if is_incremental() %}
WHERE
  DAY >= (
    SELECT
      MAX(
        DAY - INTERVAL '2 DAYS' --counteract clock skew
      )
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.day','a.pool_name']
  ) }} AS fact_daily_pool_stats_id,
  DAY,
  pool_name,
  system_rewards,
  system_rewards_usd,
  asset_liquidity,
  asset_price,
  asset_price_usd,
  cacao_liquidity,
  cacao_price,
  cacao_price_usd,
  add_liquidity_count,
  add_asset_liquidity,
  add_asset_liquidity_usd,
  add_cacao_liquidity,
  add_cacao_liquidity_usd,
  withdraw_count,
  withdraw_asset_liquidity,
  withdraw_asset_liquidity_usd,
  withdraw_cacao_liquidity,
  withdraw_cacao_liquidity_usd,
  il_protection_paid,
  il_protection_paid_usd,
  average_slip,
  to_asset_average_slip,
  to_cacao_average_slip,
  swap_count,
  to_asset_swap_count,
  to_cacao_swap_count,
  swap_volume_cacao,
  swap_volume_cacao_usd,
  to_asset_swap_volume,
  to_cacao_swap_volume,
  total_swap_fees_cacao,
  total_swap_fees_usd,
  total_asset_swap_fees,
  total_asset_cacao_fees,
  unique_member_count,
  unique_swapper_count,
  liquidity_units,
  '{{ invocation_id }}' AS _invocation_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  base A
