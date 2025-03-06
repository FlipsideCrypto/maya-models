{{ config(
  materialized = 'view'
) }}

SELECT
  tx,
  from_addr,
  node_addr,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_validator_request_leave_events'
  ) }}
