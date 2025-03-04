{{ config(
  materialized = 'incremental',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, AMM' }} },
  unique_key = 'fact_total_value_locked_id',
  incremental_predicates = ['DBT_INTERNAL_DEST.day >= (select min(day) from ' ~ generate_tmp_view_name(this) ~ ')'],
  incremental_strategy = 'merge'
) }}

WITH base AS (

  SELECT
    DAY,
    total_value_pooled,
    total_value_bonded,
    total_value_locked,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__total_value_locked') }}

{% if is_incremental() %}
WHERE
  DAY >= (
    SELECT
      MAX(
        DAY - INTERVAL '2 DAYS' --counteract clock skew
      )
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['a.day']
  ) }} AS fact_total_value_locked_id,
  DAY,
  total_value_pooled,
  total_value_bonded,
  total_value_locked,
  _INSERTED_TIMESTAMP,
  '{{ invocation_id }}' AS _invocation_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  base A
