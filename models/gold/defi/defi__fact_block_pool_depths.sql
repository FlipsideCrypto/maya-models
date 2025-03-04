{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_pool_depths_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    pool_name,
    asset_e8,
    cacao_e8,
    synth_e8,
    block_timestamp,
    _inserted_timestamp
  FROM
    {{ ref('silver__block_pool_depths') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.pool_name','a.block_timestamp']
  ) }} AS fact_pool_depths_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  cacao_e8,
  asset_e8,
  synth_e8,
  pool_name,
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
  OR pool_name IN (
    SELECT
      pool_name
    FROM
      {{ this }}
    WHERE
      dim_block_id = '-1'
  )
{% endif %}
