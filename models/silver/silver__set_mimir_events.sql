{{ config(
  materialized = 'view'
) }}

SELECT
  key,
  VALUE,
  event_id,
  block_timestamp,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__set_mimir_events') }}
  qualify(ROW_NUMBER() over(PARTITION BY event_id
ORDER BY
  __HEVO__INGESTED_AT DESC)) = 1
