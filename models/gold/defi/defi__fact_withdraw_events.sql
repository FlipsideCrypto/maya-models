{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_withdraw_events_id',
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

  SELECT
    e.tx_id,
    e.blockchain,
    e.from_address,
    e.to_address,
    e.asset,
    e.asset_e8,
    e.emit_asset_e8,
    e.emit_cacao_e8,
    e.memo,
    e.pool_name,
    e.stake_units,
    e.basis_points,
    e.asymmetry,
    e.imp_loss_protection_e8,
    e._emit_asset_in_cacao_e8,
    e.block_timestamp,
    e.event_id,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__withdraw_events') }}
    e
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.event_id']
  ) }} AS fact_withdraw_events_id,
  b.block_timestamp,
  COALESCE(
    b.dim_block_id,
    '-1'
  ) AS dim_block_id,
  A.tx_id,
  A.blockchain,
  A.from_address,
  A.to_address,
  A.asset,
  A.asset_e8,
  A.emit_asset_e8,
  A.emit_cacao_e8,
  A.memo,
  A.pool_name,
  A.stake_units,
  A.basis_points,
  A.asymmetry,
  A.imp_loss_protection_e8,
  A._emit_asset_in_cacao_e8,
  A._inserted_timestamp,
  '{{ invocation_id }}' AS _audit_run_id,
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
