{{ config(
    materialized = "table",
    schema = "prod"
) }}

WITH params AS (

    SELECT
        *
    FROM
        {{ ref('time_slots') }}
)
SELECT
    params.month_start_date,
    params.month_end_date,
    actions.role_name AS role,
    SUM(
        actions.action_count
    ) AS action_count
FROM
    params
    LEFT JOIN {{ ref('actions') }}
    ON actions.action_date >= params.month_start_date
    AND actions.action_date <= params.month_end_date
GROUP BY
    params.month_start_date,
    params.month_end_date,
    actions.role_name
