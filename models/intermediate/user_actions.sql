{{ config(
    materialized = "table",
    schema = "intermediate"
) }}

SELECT
    *
FROM
    {{ source(
        "superset",
        "logs"
    ) }}
