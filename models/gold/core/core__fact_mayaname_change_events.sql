{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_mayaname_change_events_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    NAME,
    chain,
    address,
    registration_fee_e8,
    fund_amount_e8,
    height,
    expire,
    owner,
    tx_id,
    memo,
    sender,
    preferred_asset,
    affiliate_bps,
    sub_affiliates,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__mayaname_change_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id','a.name']
  ) }} AS fact_mayaname_change_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  NAME,
  chain,
  address,
  registration_fee_e8,
  fund_amount_e8,
  height,
  expire,
  owner,
  tx_id,
  memo,
  sender,
  preferred_asset,
  affiliate_bps,
  sub_affiliates,
  event_id,
  A._INSERTED_TIMESTAMP,
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
