{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH swaps AS (

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
    streaming_count,
    streaming_quantity,
    b.block_timestamp,
    b.height AS block_id,
    A._INSERTED_TIMESTAMP,
    COUNT(1) over (
      PARTITION BY tx_id
    ) AS n_tx,
    RANK() over (
      PARTITION BY tx_id
      ORDER BY
        liq_fee_e8 ASC
    ) AS rank_liq_fee
  FROM
    {{ ref('silver__swap_events') }} A
    JOIN {{ ref('silver__block_log') }}
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
)
SELECT
  se.block_timestamp,
  se.block_id,
  tx_id,
  blockchain,
  se.pool_name,
  from_address,
  CASE
    WHEN n_tx > 1
    AND rank_liq_fee = 1
    AND len(SPLIT(memo, ':') [4] :: STRING) = 43 THEN SPLIT(
      memo,
      ':'
    ) [4] :: STRING
    WHEN n_tx > 1
    AND LOWER(LEFT(memo, 1)) IN (
      's',
      '='
    )
    AND len(COALESCE(SPLIT(memo, ':') [2] :: STRING, '')) = 0 THEN from_address
    ELSE SPLIT(
      memo,
      ':'
    ) [2] :: STRING
  END AS native_to_address,
  to_address AS to_pool_address,
  CASE
    WHEN COALESCE(SPLIT(memo, ':') [4], '') = '' THEN NULL
    WHEN CONTAINS(SPLIT(memo, ':') [4], '/') THEN SPLIT(SPLIT(memo, ':') [4], '/') [0]
    ELSE SPLIT(
      memo,
      ':'
    ) [4] :: STRING
  END AS affiliate_address,
  TRY_CAST(
    CASE
      WHEN COALESCE(SPLIT(memo, ':') [5], '') = '' THEN NULL
      WHEN CONTAINS(SPLIT(memo, ':') [5], '/') THEN SPLIT(SPLIT(memo, ':') [5], '/') [0]
      ELSE SPLIT(
        memo,
        ':'
      ) [5]
    END :: STRING AS INT
  ) AS affiliate_fee_basis_points,
  SPLIT(COALESCE(SPLIT(SPLIT(memo, '|') [0], ':') [4], ''), '/') AS affiliate_addresses_array,
  ARRAY_AGG(TRY_CAST(TRIM(f.value) AS INTEGER)) within GROUP (
    ORDER BY
      f.index
  ) AS affiliate_fee_basis_points_array,
  from_asset,
  to_asset,
  COALESCE(from_e8 / pow(10, 8), 0) AS from_amount,
  COALESCE(to_e8 / pow(10, 8), 0) AS to_amount,
  COALESCE(to_e8_min / pow(10, 8), 0) AS min_to_amount,
  CASE
    WHEN from_asset = 'MAYA.CACAO' THEN COALESCE(from_e8 * cacao_usd / pow(10, 8), 0)
    ELSE COALESCE(from_e8 * asset_usd / pow(10, 8), 0)
  END AS from_amount_usd,
  CASE
    WHEN to_asset = 'MAYA.CACAO' THEN COALESCE(to_e8 * cacao_usd / pow(10, 8), 0)
    ELSE COALESCE(to_e8 * asset_usd / pow(10, 8), 0)
  END AS to_amount_usd,
  cacao_usd,
  asset_usd,
  CASE
    WHEN to_asset = 'MAYA.CACAO' THEN COALESCE(to_e8_min * cacao_usd / pow(10, 8), 0)
    ELSE COALESCE(to_e8_min * asset_usd / pow(10, 8), 0)
  END AS to_amount_min_usd,
  swap_slip_bp,
  COALESCE(liq_fee_in_cacao_e8 / pow(10, 8), 0) AS liq_fee_cacao,
  COALESCE(liq_fee_in_cacao_e8 / pow(10, 8) * cacao_usd, 0) AS liq_fee_cacao_usd,
  CASE
    WHEN to_asset = 'MAYA.CACAO' THEN COALESCE(liq_fee_e8 / pow(10, 8), 0)
    ELSE COALESCE(liq_fee_e8 / pow(10, 8), 0)
  END AS liq_fee_asset,
  CASE
    WHEN to_asset = 'MAYA.CACAO' THEN COALESCE(liq_fee_e8 * cacao_usd / pow(10, 8), 0)
    ELSE COALESCE(liq_fee_e8 * asset_usd / pow(10, 8), 0)
  END AS liq_fee_asset_usd,
  streaming_count,
  streaming_quantity,
  concat_ws(
    '-',
    tx_id,
    se.block_id,
    to_asset,
    from_asset,
    COALESCE(
      native_to_address,
      ''
    ),
    from_address,
    se.pool_name,
    to_pool_address,
    event_id
  ) AS _unique_key,
  _INSERTED_TIMESTAMP
FROM
  swaps se
  LEFT JOIN {{ ref('silver__prices') }}
  p
  ON se.block_id = p.block_id
  AND se.pool_name = p.pool_name,
  LATERAL FLATTEN(
    input => SPLIT(COALESCE(SPLIT(memo, ':') [5], ''), '/')
  ) f
GROUP BY
  ALL
