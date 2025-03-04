{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_block_rewards_id',
  incremental_predicates = ['DBT_INTERNAL_DEST.day >= (select min(day) from ' ~ generate_tmp_view_name(this) ~ ')'],
  incremental_strategy = 'merge'
) }}

WITH base AS (

  SELECT
    DAY,
    liquidity_fee,
    block_rewards,
    earnings,
    bonding_earnings,
    liquidity_earnings,
    avg_node_count,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__block_rewards') }}

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
    ['a.day']
  ) }} AS fact_block_rewards_id,
  DAY,
  liquidity_fee,
  block_rewards,
  earnings,
  bonding_earnings,
  liquidity_earnings,
  avg_node_count,
  A._inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  base A
