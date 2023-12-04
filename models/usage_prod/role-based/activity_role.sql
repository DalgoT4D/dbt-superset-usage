{{ config(
    materialized = "table",
    schema = "usage_prod"
) }}

WITH params AS (

    SELECT
        *
    FROM
        {{ ref('time_slots') }}
        CROSS JOIN (
            SELECT
                dashboard_title
            FROM
                {{ ref('dashboards') }}
            WHERE
                published IS TRUE
            UNION
            SELECT
                'All' AS dashboard_title
        ) dashboards
        CROSS JOIN (
            SELECT
                org
            FROM
                {{ ref('orgs') }}
        ) org
),
actions_all_dashboards AS (
    SELECT
        action_id,
        user_id,
        user_name,
        user_created_on,
        role_name,
        action,
        action_count,
        action_date,
        CASE
            WHEN dashboard_title IS NOT NULL THEN 'All'
            ELSE dashboard_title
        END AS dashboard_title,
        dashboard_created_on,
        ROW_NUMBER() over (
            PARTITION BY action_id
        ) AS row_no,
        org
    FROM
        {{ ref('actions') }}
),
actions AS (
    SELECT
        action_id,
        user_id,
        user_name,
        user_created_on,
        role_name,
        action,
        action_count,
        action_date,
        dashboard_title,
        dashboard_created_on,
        1 AS row_no,
        org
    FROM
        {{ ref('actions') }}
    UNION ALL
    SELECT
        *
    FROM
        actions_all_dashboards
    WHERE
        row_no = 1
)
SELECT
    params.month_start_date,
    params.month_end_date,
    params.org,
    actions.dashboard_title,
    actions.role_name AS role,
    SUM(
        actions.action_count
    ) AS action_count,
    COUNT(
        DISTINCT user_name
    ) AS active_users
FROM
    params
    LEFT JOIN actions
    ON actions.action_date >= params.month_start_date
    AND actions.action_date <= params.month_end_date
    AND actions.org = params.org
GROUP BY
    params.month_start_date,
    params.month_end_date,
    params.org,
    actions.dashboard_title,
    actions.role_name
