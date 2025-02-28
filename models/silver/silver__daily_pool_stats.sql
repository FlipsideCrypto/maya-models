{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
  cluster_by = ['day'],
  incremental_predicates = ['DBT_INTERNAL_DEST.day >= (select min(day) from ' ~ generate_tmp_view_name(this) ~ ')']
) }}

WITH daily_cacao_price AS (

  SELECT
    pool_name,
    block_timestamp :: DATE AS DAY,
    AVG(cacao_usd) AS cacao_usd,
    AVG(asset_usd) AS asset_usd
  FROM
    {{ ref('silver__prices') }}
    p

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
  pool_name,
  DAY
),
pool_fees AS (
  SELECT
    pbf.day,
    pbf.pool_name,
    rewards AS system_rewards,
    rewards * cacao_usd AS system_rewards_usd,
    asset_liquidity_fees,
    asset_liquidity_fees * asset_usd AS asset_liquidity_fees_usd,
    cacao_liquidity_fees,
    cacao_liquidity_fees * cacao_usd AS cacao_liquidity_fees_usd
  FROM
    {{ ref('silver__pool_block_fees') }}
    pbf
    JOIN daily_cacao_price drp
    ON pbf.day = drp.day
    AND pbf.pool_name = drp.pool_name

{% if is_incremental() %}
WHERE
  pbf.day >= (
    SELECT
      MAX(
        DAY - INTERVAL '2 DAYS' --counteract clock skew
      )
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  pbs.day,
  pbs.asset AS pool_name,
  COALESCE(
    system_rewards,
    0
  ) AS system_rewards,
  COALESCE(
    system_rewards_usd,
    0
  ) AS system_rewards_usd,
  COALESCE(asset_depth / pow(10, 8), 0) AS asset_liquidity,
  COALESCE(
    asset_price,
    0
  ) AS asset_price,
  COALESCE(
    asset_price_usd,
    0
  ) AS asset_price_usd,
  COALESCE(cacao_depth / pow(10, 8), 0) AS cacao_liquidity,
  COALESCE(asset_price_usd / NULLIF(cacao_usd, 0), 0) AS cacao_price,
  COALESCE(
    cacao_usd,
    0
  ) AS cacao_price_usd,
  COALESCE(
    add_liquidity_count,
    0
  ) AS add_liquidity_count,
  COALESCE(add_asset_liquidity_volume / pow(10, 8), 0) AS add_asset_liquidity,
  COALESCE(add_asset_liquidity_volume / pow(10, 8) * asset_usd, 0) AS add_asset_liquidity_usd,
  COALESCE(add_cacao_liquidity_volume / pow(10, 8), 0) AS add_cacao_liquidity,
  COALESCE(add_cacao_liquidity_volume / pow(10, 8) * cacao_usd, 0) AS add_cacao_liquidity_usd,
  COALESCE(
    withdraw_count,
    0
  ) AS withdraw_count,
  COALESCE(withdraw_asset_volume / pow(10, 8), 0) AS withdraw_asset_liquidity,
  COALESCE(withdraw_asset_volume / pow(10, 8) * asset_usd, 0) AS withdraw_asset_liquidity_usd,
  COALESCE(withdraw_cacao_volume / pow(10, 8), 0) AS withdraw_cacao_liquidity,
  COALESCE(withdraw_cacao_volume / pow(10, 8) * cacao_usd, 0) AS withdraw_cacao_liquidity_usd,
  COALESCE(impermanent_loss_protection_paid / pow(10, 8), 0) AS il_protection_paid,
  COALESCE(impermanent_loss_protection_paid / pow(10, 8) * cacao_usd, 0) AS il_protection_paid_usd,
  COALESCE(
    average_slip,
    0
  ) AS average_slip,
  COALESCE(
    to_asset_average_slip,
    0
  ) AS to_asset_average_slip,
  COALESCE(
    to_cacao_average_slip,
    0
  ) AS to_cacao_average_slip,
  COALESCE(
    swap_count,
    0
  ) AS swap_count,
  COALESCE(
    to_asset_count,
    0
  ) AS to_asset_swap_count,
  COALESCE(
    to_cacao_count,
    0
  ) AS to_cacao_swap_count,
  COALESCE(swap_volume / pow(10, 8), 0) AS swap_volume_cacao,
  COALESCE(swap_volume / pow(10, 8) * cacao_usd, 0) AS swap_volume_cacao_usd,
  COALESCE(to_asset_volume / pow(10, 8), 0) AS to_asset_swap_volume,
  COALESCE(to_cacao_volume / pow(10, 8), 0) AS to_cacao_swap_volume,
  COALESCE(totalfees / pow(10, 8), 0) AS total_swap_fees_cacao,
  COALESCE(totalfees / pow(10, 8) * cacao_usd, 0) AS total_swap_fees_usd,
  COALESCE(to_asset_fees / pow(10, 8), 0) AS total_asset_swap_fees,
  COALESCE(to_cacao_fees / pow(10, 8), 0) AS total_asset_cacao_fees,
  COALESCE(
    unique_member_count,
    0
  ) AS unique_member_count,
  COALESCE(
    unique_swapper_count,
    0
  ) AS unique_swapper_count,
  COALESCE(
    units,
    0
  ) AS liquidity_units,
  concat_ws(
    '-',
    pbs.day,
    pbs.asset
  ) AS _unique_key
FROM
  {{ ref('silver__pool_block_statistics') }}
  pbs
  LEFT JOIN daily_cacao_price drp
  ON pbs.day = drp.day
  AND pbs.asset = drp.pool_name
  LEFT JOIN pool_fees pf
  ON pbs.day = pf.day
  AND pbs.asset = pf.pool_name

{% if is_incremental() %}
WHERE
  pbs.day >= (
    SELECT
      MAX(
        DAY - INTERVAL '2 DAYS' --counteract clock skew
      )
    FROM
      {{ this }}
  )
{% endif %}
