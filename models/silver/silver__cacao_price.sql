{{ config(
  materialized = 'view'
) }}

SELECT
  cacao_price_e8,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref(
    'bronze__cacao_price'
  ) }}
  qualify(ROW_NUMBER() over (PARTITION BY block_timestamp
ORDER BY
  cacao_price_e8 DESC) = 1)
