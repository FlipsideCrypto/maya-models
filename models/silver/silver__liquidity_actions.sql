{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH stakes AS (

  SELECT
    block_timestamp,
    cacao_tx_id,
    pool_name,
    cacao_address,
    cacao_e8,
    asset_e8,
    stake_units,
    asset_tx_id,
    asset_address,
    asset_blockchain,
    event_id,
    _inserted_timestamp
  FROM
    {{ ref('silver__stake_events') }}
),
unstakes AS (
  SELECT
    block_timestamp,
    tx_id,
    pool_name,
    from_address,
    to_address,
    emit_cacao_e8,
    emit_asset_e8,
    stake_units,
    imp_loss_protection_e8,
    asymmetry,
    basis_points,
    event_id,
    _inserted_timestamp
  FROM
    {{ ref('silver__withdraw_events') }}
)
SELECT
  b.block_timestamp,
  b.height AS block_id,
  cacao_tx_id AS tx_id,
  'add_liquidity' AS lp_action,
  se.pool_name,
  cacao_address AS from_address,
  NULL AS to_address,
  COALESCE((cacao_e8 / pow(10, 8)), 0) AS cacao_amount,
  COALESCE((cacao_e8 / pow(10, 8) * cacao_usd), 0) AS cacao_amount_usd,
  COALESCE((asset_e8 / pow(10, 8)), 0) AS asset_amount,
  COALESCE((asset_e8 / pow(10, 8) * asset_usd), 0) AS asset_amount_usd,
  stake_units,
  asset_tx_id,
  asset_address,
  asset_blockchain,
  NULL AS il_protection,
  NULL AS il_protection_usd,
  NULL AS unstake_asymmetry,
  NULL AS unstake_basis_points,
  concat_ws(
    '-',
    event_id,
    se.block_timestamp,
    COALESCE(
      tx_id,
      ''
    ),
    lp_action,
    se.pool_name,
    COALESCE(
      from_address,
      ''
    ),
    COALESCE(
      to_address,
      ''
    ),
    COALESCE(
      asset_tx_id,
      ''
    )
  ) AS _unique_key,
  se._inserted_timestamp
FROM
  stakes se
  JOIN {{ ref('silver__block_log') }}
  b
  ON se.block_timestamp = b.timestamp
  LEFT JOIN {{ ref('silver__prices') }}
  p
  ON b.height = p.block_id
  AND se.pool_name = p.pool_name

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
UNION
SELECT
  b.block_timestamp,
  b.height AS block_id,
  tx_id,
  'remove_liquidity' AS lp_action,
  ue.pool_name,
  from_address,
  to_address,
  COALESCE(emit_cacao_e8 / pow(10, 8), 0) AS cacao_amount,
  COALESCE(emit_cacao_e8 / pow(10, 8) * cacao_usd, 0) AS cacao_amount_usd,
  COALESCE(emit_asset_e8 / pow(10, 8), 0) AS asset_amount,
  COALESCE(emit_asset_e8 / pow(10, 8) * asset_usd, 0) AS asset_amount_usd,
  stake_units,
  NULL AS asset_tx_id,
  NULL AS asset_address,
  NULL AS asset_blockchain,
  imp_loss_protection_e8 / pow(
    10,
    8
  ) AS il_protection,
  imp_loss_protection_e8 / pow(
    10,
    8
  ) * cacao_usd AS il_protection_usd,
  asymmetry AS unstake_asymmetry,
  basis_points AS unstake_basis_points,
  concat_ws(
    '-',
    event_id,
    ue.block_timestamp,
    COALESCE(
      tx_id,
      ''
    ),
    lp_action,
    ue.pool_name,
    COALESCE(
      from_address,
      ''
    ),
    COALESCE(
      to_address,
      ''
    ),
    COALESCE(
      asset_tx_id,
      ''
    )
  ) AS _unique_key,
  ue._inserted_timestamp
FROM
  unstakes ue
  JOIN {{ ref('silver__block_log') }}
  b
  ON ue.block_timestamp = b.timestamp
  LEFT JOIN {{ ref('silver__prices') }}
  p
  ON b.height = p.block_id
  AND ue.pool_name = p.pool_name

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
