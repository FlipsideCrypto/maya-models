{{ config(
  materialized = 'view'
) }}

SELECT
  NAME,
  chain,
  address,
  registration_fee_e8,
  fund_amount_e8,
  height,
  expire,
  owner,
  tx_id,
  memo,
  sender,
  preferred_asset,
  affiliate_bps,
  sub_affiliates,
  event_id,
  block_timestamp,
  __HEVO__DATABASE_NAME,
  __HEVO__SCHEMA_NAME,
  __HEVO__INGESTED_AT,
  __HEVO__LOADED_AT,
FROM
  {{ source(
    'maya_midgard',
    'midgard_mayaname_change_events'
  ) }}
