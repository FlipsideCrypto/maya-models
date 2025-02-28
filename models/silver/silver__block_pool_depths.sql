{{ config(
  materialized = 'view'
) }}

SELECT
  pool AS pool_name,
  asset_e8,
  cacao_e8,
  synth_e8,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__block_pool_depths') }}
  qualify(ROW_NUMBER() over(PARTITION BY pool, block_timestamp
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
