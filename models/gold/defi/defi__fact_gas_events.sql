{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_gas_events_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    asset,
    asset_e8,
    cacao_e8,
    tx_count,
    event_id,
    block_timestamp,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__gas_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id','a.asset','a.block_timestamp']
  ) }} AS fact_gas_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  asset,
  asset_e8,
  cacao_e8,
  tx_count,
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
  OR asset IN (
    SELECT
      asset
    FROM
      {{ this }}
    WHERE
      dim_block_id = '-1'
  )
{% endif %}
