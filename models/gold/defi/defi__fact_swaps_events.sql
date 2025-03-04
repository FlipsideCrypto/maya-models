{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM, SWAPS' }} },
  unique_key = 'fact_swap_events_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    tx_id,
    blockchain,
    from_address,
    to_address,
    from_asset,
    from_e8,
    to_asset,
    to_e8,
    memo,
    pool_name,
    to_e8_min,
    swap_slip_bp,
    liq_fee_e8,
    liq_fee_in_cacao_e8,
    _DIRECTION,
    event_id,
    block_timestamp,
    streaming_count,
    streaming_quantity,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__swap_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id','a.tx_id','a.blockchain','a.to_address','a.from_address','a.from_asset','a.from_e8','a.to_asset','a.to_e8','a.memo','a.pool_name','a._direction']
  ) }} AS fact_swap_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  blockchain,
  from_address,
  to_address,
  from_asset,
  from_e8,
  to_asset,
  to_e8,
  memo,
  pool_name,
  to_e8_min,
  swap_slip_bp,
  liq_fee_e8,
  liq_fee_in_cacao_e8,
  _DIRECTION,
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
