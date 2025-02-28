{{ config(
  materialized = 'view'
) }}

SELECT
  node_addr,
  former,
  "CURRENT",
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_update_node_account_status_events'
  ) }}
