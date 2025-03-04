{{ config(
  materialized = 'incremental',
  unique_key = "_unique_key",
  incremental_strategy = 'merge',
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
)
SELECT
  b.block_timestamp,
  b.height AS block_id,
  from_address,
  to_address,
  asset,
  COALESCE(amount_e8 / pow(10, 8), 0) AS cacao_amount,
  COALESCE(amount_e8 / pow(10, 8) * cacao_usd, 0) AS cacao_amount_usd,
  event_id,
  {{ dbt_utils.generate_surrogate_key(
    ['se.event_id', 'se.from_address', 'se.to_address', 'se.asset', 'se.amount_e8']
  ) }}
  _unique_key,
  se._inserted_timestamp
FROM
  {{ ref('silver__transfer_events') }}
  se
  JOIN {{ ref('silver__block_log') }}
  b
  ON se.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.height = p.block_id

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
