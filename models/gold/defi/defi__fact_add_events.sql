{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_add_events_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    e.block_timestamp,
    e.tx_id,
    e.cacao_e8,
    e.blockchain,
    e.asset_e8,
    e.pool_name,
    e.memo,
    e.to_address,
    e.from_address,
    e.asset,
    e.event_id,
    _inserted_timestamp
  FROM
    {{ ref('silver__add_events') }}
    e
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id','a.tx_id','a.blockchain','a.from_address','a.to_address','a.asset','a.memo','a.block_timestamp']
  ) }} AS fact_add_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.tx_id,
  A.cacao_e8,
  A.blockchain,
  A.asset_e8,
  A.pool_name,
  A.memo,
  A.to_address,
  A.from_address,
  A.asset,
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
  OR tx_id IN (
    SELECT
      tx_id
    FROM
      {{ this }}
    WHERE
      dim_block_id = '-1'
  )
{% endif %}
