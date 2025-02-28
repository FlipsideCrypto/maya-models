{{ config(
  materialized = 'incremental',
  unique_key = 'day',
  incremental_strategy = 'merge',
  cluster_by = ['day']
) }}

WITH max_daily_block AS (

  SELECT
    MAX(
      block_id
    ) AS block_id,
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DAY
  FROM
    {{ ref('silver__prices') }} A

{% if is_incremental() %}
WHERE
  block_timestamp :: DATE >= (
    SELECT
      MAX(
        DAY - INTERVAL '2 DAYS' --counteract clock skew
      )
    FROM
      {{ this }}
  )
{% endif %}
GROUP BY
  DAY
),
daily_cacao_price AS (
  SELECT
    p.block_id,
    DAY,
    AVG(cacao_usd) AS cacao_usd
  FROM
    {{ ref('silver__prices') }}
    p
    JOIN max_daily_block mdb
    ON p.block_id = mdb.block_id

{% if is_incremental() %}
WHERE
  block_timestamp :: DATE >= (
    SELECT
      MAX(
        DAY - INTERVAL '2 DAYS' --counteract clock skew
      )
    FROM
      {{ this }}
  )
{% endif %}
GROUP BY
  DAY,
  p.block_id
)
SELECT
  br.day,
  COALESCE(
    liquidity_fee,
    0
  ) AS liquidity_fees,
  COALESCE(
    liquidity_fee * cacao_usd,
    0
  ) AS liquidity_fees_usd,
  block_rewards AS block_rewards,
  block_rewards * cacao_usd AS block_rewards_usd,
  COALESCE(
    earnings,
    0
  ) AS total_earnings,
  COALESCE(
    earnings * cacao_usd,
    0
  ) AS total_earnings_usd,
  bonding_earnings AS earnings_to_nodes,
  bonding_earnings * cacao_usd AS earnings_to_nodes_usd,
  COALESCE(
    liquidity_earnings,
    0
  ) AS earnings_to_pools,
  COALESCE(
    liquidity_earnings * cacao_usd,
    0
  ) AS earnings_to_pools_usd,
  avg_node_count,
  br._inserted_timestamp
FROM
  {{ ref('silver__block_rewards') }}
  br
  JOIN daily_cacao_price drp
  ON br.day = drp.day

{% if is_incremental() %}
WHERE
  br.day >= (
    SELECT
      MAX(
        DAY
      )
    FROM
      {{ this }}
  )
{% endif %}
