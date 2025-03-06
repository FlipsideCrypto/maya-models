{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
  incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp >= (select min(block_timestamp) from ' ~ generate_tmp_view_name(this) ~ ')'],
  cluster_by = ['block_timestamp::DATE']
) }}
-- block level prices by pool
-- step 1 what is the USD pool with the highest balance (aka deepest pool)
WITH blocks AS (

  SELECT
    height AS block_id,
    b.block_timestamp,
    pool_name,
    cacao_e8,
    asset_e8
  FROM
    {{ ref('silver__block_pool_depths') }} A
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
),
price AS (
  SELECT
    height AS block_id,
    b.block_timestamp,
    cacao_price_e8 AS cacao_usd
  FROM
    {{ ref('silver__cacao_price') }} A
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
) -- step 3 calculate the prices of assets by pool, in terms of tokens per tokens
-- and in USD for both tokens
SELECT
  DISTINCT b.block_id,
  b.block_timestamp,
  COALESCE(
    cacao_e8 / asset_e8,
    0
  ) AS price_cacao_asset,
  COALESCE(
    asset_e8 / cacao_e8,
    0
  ) AS price_asset_cacao,
  COALESCE(cacao_usd * (cacao_e8 / asset_e8), 0) AS asset_usd,
  COALESCE(
    cacao_usd,
    0
  ) AS cacao_usd,
  pool_name,
  concat_ws(
    '-',
    b.block_id :: STRING,
    pool_name :: STRING
  ) AS _unique_key
FROM
  blocks b
  JOIN price ru
  ON b.block_id = ru.block_id
WHERE
  cacao_e8 > 0
  AND asset_e8 > 0
