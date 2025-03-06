{{ config(
  materialized = 'view'
) }}

SELECT
  node_addr AS node_address,
  "CURRENT" AS current_status,
  former AS former_status,
  block_timestamp,
  event_id,
  DATEADD(
    ms,
    __HEVO__LOADED_AT,
    '1970-01-01'
  ) AS _INSERTED_TIMESTAMP
FROM
  {{ ref('bronze__update_node_account_status_events') }}
  e qualify(ROW_NUMBER() over(PARTITION BY event_id
ORDER BY
  __HEVO__LOADED_AT DESC)) = 1
