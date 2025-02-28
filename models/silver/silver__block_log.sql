{{ config(
  materialized = 'view'
) }}

WITH base AS (

  SELECT
    height,
    TO_TIMESTAMP(
      TIMESTAMP / 1000000000
    ) AS block_timestamp,
    TIMESTAMP,
    HASH,
    agg_state,
    DATEADD(
      ms,
      __HEVO__LOADED_AT,
      '1970-01-01'
    ) AS _INSERTED_TIMESTAMP
  FROM
    {{ ref('bronze__block_log') }}
    qualify(ROW_NUMBER() over(PARTITION BY height
  ORDER BY
    __HEVO__LOADED_AT DESC)) = 1
)
SELECT
  height,
  block_timestamp,
  block_timestamp :: DATE AS block_date,
  HOUR(block_timestamp) AS block_hour,
  week(block_timestamp) AS block_week,
  MONTH(block_timestamp) AS block_month,
  quarter(block_timestamp) AS block_quarter,
  YEAR(block_timestamp) AS block_year,
  dayofmonth(block_timestamp) AS block_DAYOFMONTH,
  dayofweek(block_timestamp) AS block_DAYOFWEEK,
  dayofyear(block_timestamp) AS block_DAYOFYEAR,
  TIMESTAMP,
  HASH,
  agg_state,
  _INSERTED_TIMESTAMP
FROM
  base
