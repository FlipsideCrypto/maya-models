{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = "fact_bond_actions_id",
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH block_prices AS (

  SELECT
    AVG(cacao_usd) AS cacao_usd,
    block_id
  FROM
    {{ ref('silver__prices') }}
  GROUP BY
    block_id
),
bond_events AS (
  SELECT
    block_timestamp,
    tx_id,
    from_address,
    to_address,
    asset,
    blockchain,
    bond_type,
    asset_e8,
    e8,
    memo,
    event_id,
    _inserted_timestamp
  FROM
    {{ ref('silver__bond_events') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['be.event_id']
  ) }} AS fact_bond_actions_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  tx_id,
  from_address,
  to_address,
  asset,
  blockchain,
  bond_type,
  COALESCE(e8 / pow(10, 8), 0) AS asset_amount,
  COALESCE(
    cacao_usd * asset_e8,
    0
  ) AS asset_usd,
  memo,
  be._inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  bond_events be
  JOIN {{ ref('core__dim_block') }}
  b
  ON be.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.block_id = p.block_id

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
