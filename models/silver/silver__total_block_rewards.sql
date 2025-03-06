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
),
fin AS (
  SELECT
    b.block_timestamp,
    b.height AS block_id,
    ree.pool_name AS reward_entity,
    COALESCE(cacao_e8 / pow(10, 8), 0) AS cacao_amount,
    COALESCE(cacao_e8 / pow(10, 8) * cacao_usd, 0) AS cacao_amount_usd,
    concat_ws(
      '-',
      b.height,
      reward_entity
    ) AS _unique_key,
    ree._inserted_timestamp
  FROM
    {{ ref('silver__rewards_event_entries') }}
    ree
    JOIN {{ ref('silver__block_log') }}
    b
    ON ree.block_timestamp = b.timestamp
    LEFT JOIN {{ ref('silver__prices') }}
    p
    ON b.height = p.block_id
    AND ree.pool_name = p.pool_name

{% if is_incremental() %}
WHERE
  (
    b.block_timestamp >= (
      SELECT
        MAX(
          block_timestamp - INTERVAL '1 HOUR'
        )
      FROM
        {{ this }}
    )
    OR concat_ws(
      '-',
      b.height,
      reward_entity
    ) IN (
      SELECT
        _unique_key
      FROM
        {{ this }}
      WHERE
        cacao_amount_USD IS NULL
    )
  )
{% endif %}
UNION
SELECT
  b.block_timestamp,
  b.height AS block_id,
  'bond_holders' AS reward_entity,
  bond_e8 / pow(
    10,
    8
  ) AS cacao_amount,
  bond_e8 / pow(
    10,
    8
  ) * cacao_usd AS cacao_amount_usd,
  concat_ws(
    '-',
    b.height,
    reward_entity
  ) AS _unique_key,
  re._inserted_timestamp
FROM
  {{ ref('silver__rewards_events') }}
  re
  JOIN {{ ref('silver__block_log') }}
  b
  ON re.block_timestamp = b.timestamp
  LEFT JOIN block_prices p
  ON b.height = p.block_id

{% if is_incremental() %}
WHERE
  (
    b.block_timestamp >= (
      SELECT
        MAX(
          block_timestamp - INTERVAL '1 HOUR'
        )
      FROM
        {{ this }}
    )
    OR concat_ws(
      '-',
      b.height,
      reward_entity
    ) IN (
      SELECT
        _unique_key
      FROM
        {{ this }}
      WHERE
        cacao_amount_USD IS NULL
    )
  )
{% endif %}
)
SELECT
  block_timestamp,
  block_id,
  reward_entity,
  SUM(
    cacao_amount
  ) AS cacao_amount,
  SUM(cacao_amount_usd) AS cacao_amount_usd,
  _unique_key,
  MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
  fin
GROUP BY
  block_timestamp,
  block_id,
  reward_entity,
  _unique_key
