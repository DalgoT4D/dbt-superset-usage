{{ config(
    materialized = "table",
    schema = "prod"
) }}

WITH cte AS (

    SELECT
        user_id,
        user_name,
        role_name,
        chart_title,
        dashboard_title,
        action_count,
        action,
        DATE_TRUNC(
            'month',
            action_date
        ) AS datemonth
    FROM
        {{ ref('actions') }}
    UNION ALL
    SELECT
        user_id,
        user_name,
        role_name,
        chart_title,
        dashboard_title,
        action_count,
        action,
        DATE_TRUNC(
            'month',
            action_date
        ) AS datemonth
    FROM
        {{ ref('actions_all') }}
)
SELECT
    user_id,
    user_name,
    role_name,
    chart_title,
    dashboard_title,
    action,
    datemonth,
    SUM(action_count) AS action_count
FROM
    cte
GROUP BY
    user_id,
    user_name,
    role_name,
    chart_title,
    dashboard_title,
    action,
    datemonth
