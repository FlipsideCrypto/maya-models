{{ config(
  materialized = 'incremental',
  unique_key = '_unique_key',
  incremental_strategy = 'merge',
  cluster_by = ['block_timestamp::DATE']
) }}

SELECT
  b.block_timestamp,
  b.height AS block_id,
  bpd.pool_name,
  COALESCE(cacao_e8 / pow(10, 8), 0) AS cacao_amount,
  COALESCE(cacao_e8 / pow(10, 8) * cacao_usd, 0) AS cacao_amount_usd,
  COALESCE(asset_e8 / pow(10, 8), 0) AS asset_amount,
  COALESCE(asset_e8 / pow(10, 8) * asset_usd, 0) AS asset_amount_usd,
  COALESCE(synth_e8 / pow(10, 8), 0) AS synth_amount,
  COALESCE(synth_e8 / pow(10, 8) * asset_usd, 0) AS synth_amount_usd,
  concat_ws(
    '-',
    bpd.block_timestamp,
    bpd.pool_name
  ) AS _unique_key,
  bpd._inserted_timestamp
FROM
  {{ ref('silver__block_pool_depths') }}
  bpd
  JOIN {{ ref('silver__block_log') }}
  b
  ON bpd.block_timestamp = b.timestamp
  LEFT JOIN {{ ref('silver__prices') }}
  p
  ON b.height = p.block_id
  AND bpd.pool_name = p.pool_name

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
