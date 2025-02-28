{{ config(
  materialized = 'view'
) }}

SELECT
  node_addr,
  secp256k1,
  ed25519,
  validator_consensus,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_set_node_keys_events'
  ) }}
