{{ config(
  materialized = 'view'
) }}

SELECT
  add_asgard_addr,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__active_vault_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
