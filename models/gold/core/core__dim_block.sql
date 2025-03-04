{{ config(
    materialized = 'incremental',
    unique_key = 'dim_block_id',
    incremental_strategy = 'merge',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_id >= (select min(block_id) from ' ~ generate_tmp_view_name(this) ~ ')'],
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['height']
    ) }} AS dim_block_id,
    height AS block_id,
    block_timestamp,
    block_date,
    block_hour,
    block_week,
    block_month,
    block_quarter,
    block_year,
    block_DAYOFMONTH,
    block_DAYOFWEEK,
    block_DAYOFYEAR,
    TIMESTAMP,
    HASH,
    agg_state,
    _INSERTED_TIMESTAMP,
    '{{ invocation_id }}' AS _invocation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__block_log') }}

{% if is_incremental() %}
WHERE
    block_id >= (
        SELECT
            MAX(
                block_id - 600 --about 1 hour
            )
        FROM
            {{ this }}
    )
UNION ALL
SELECT
    '-1' AS dim_block_id,
    -1 AS block_id,
    '1900-01-01' :: datetime AS block_timestamp,
    NULL AS block_date,
    NULL AS block_hour,
    NULL AS block_week,
    NULL AS block_month,
    NULL AS block_quarter,
    NULL AS block_year,
    NULL AS block_DAYOFMONTH,
    NULL AS block_DAYOFWEEK,
    NULL AS block_DAYOFYEAR,
    NULL AS TIMESTAMP,
    NULL AS HASH,
    NULL AS agg_state,
    '1900-01-01' :: DATE AS _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    '1900-01-01' :: DATE AS inserted_timestamp,
    '1900-01-01' :: DATE AS modified_timestamp
UNION ALL
SELECT
    '-2' AS dim_block_id,
    -2 AS block_id,
    NULL AS block_timestamp,
    NULL AS block_date,
    NULL AS block_hour,
    NULL AS block_week,
    NULL AS block_month,
    NULL AS block_quarter,
    NULL AS block_year,
    NULL AS block_DAYOFMONTH,
    NULL AS block_DAYOFWEEK,
    NULL AS block_DAYOFYEAR,
    NULL AS TIMESTAMP,
    NULL AS HASH,
    NULL AS agg_state,
    '1900-01-01' :: DATE AS _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    '1900-01-01' :: DATE AS inserted_timestamp,
    '1900-01-01' :: DATE AS modified_timestamp
{% endif %}
