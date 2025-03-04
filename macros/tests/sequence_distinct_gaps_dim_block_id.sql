{% macro sequence_distinct_gaps_dim_block_id(
        table,
        column
    ) %}
    {%- set partition_sql = partition_by | join(", ") -%}
    {%- set previous_column = "prev_" ~ column -%}
    WITH source AS (
        SELECT
            {{ partition_sql + "," if partition_sql }}
            {{ column }},
            LAG(
                {{ column }},
                1
            ) over (
                ORDER BY
                    {{ column }} ASC
            ) AS {{ previous_column }}
        FROM
            (
                SELECT
                    DISTINCT {{ column }}
                FROM
                    {{ table }} A
                    JOIN {{ ref('core__dim_block') }}
                    b
                    ON A.dim_block_id = b.dim_block_id
                WHERE
                    A.dim_block_id <> '-1'
                    AND b.block_timestamp :: DATE < CURRENT_DATE
            )
    )
SELECT
    {{ previous_column }},
    {{ column }},
    {{ column }} - {{ previous_column }}
    - 1 AS gap
FROM
    source
WHERE
    {{ column }} - {{ previous_column }} <> 1
ORDER BY
    gap DESC
{% endmacro %}
