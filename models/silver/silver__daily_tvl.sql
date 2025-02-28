{{ config(
  materialized = 'incremental',
  unique_key = "day",
  incremental_strategy = 'merge',
  cluster_by = ['day'],
  incremental_predicates = ['DBT_INTERNAL_DEST.day >= (select min(day) from ' ~ generate_tmp_view_name(this) ~ ')']
) }}

WITH max_daily_block AS (

  SELECT
    MAX(block_id) AS block_id,
    DATE_TRUNC(
      'day',
      block_timestamp
    ) AS DAY
  FROM
    {{ ref('silver__prices') }}

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
  total_value_pooled AS total_value_pooled,
  total_value_pooled * cacao_usd AS total_value_pooled_usd,
  total_value_bonded AS total_value_bonded,
  total_value_bonded * cacao_usd AS total_value_bonded_usd,
  total_value_locked AS total_value_locked,
  total_value_locked * cacao_usd AS total_value_locked_usd
FROM
  {{ ref('silver__total_value_locked') }}
  br
  JOIN daily_cacao_price drp
  ON br.day = drp.day
