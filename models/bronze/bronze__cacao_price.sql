{{ config(
  materialized = 'view'
) }}

SELECT
  cacao_price_e8,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_cacao_price'
  ) }}
