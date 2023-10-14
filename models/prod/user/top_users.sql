{{ config(
    materialized = "table",
    schema = "prod"
) }}

WITH params AS (

    SELECT
        *
    FROM
        {{ ref('time_slots') }}
        CROSS JOIN (
            SELECT
                "name" AS role_name
            FROM
                {{ source(
                    'superset',
                    'roles'
                ) }}
            UNION
            SELECT
                'All' AS role_name
        ) roles
        CROSS JOIN (
            SELECT
                dashboard_title
            FROM
                {{ source(
                    'superset',
                    'dashboards'
                ) }}
            WHERE
                published IS TRUE
            UNION
            SELECT
                'All' AS dashboard_title
        ) dashboards
),
actions_all_roles AS (
    SELECT
        action_id,
        user_id,
        user_name,
        user_created_on,
        CASE
            WHEN role_name IS NOT NULL THEN 'All'
            ELSE role_name
        END AS role_name,
        action,
        action_count,
        action_date,
        chart_title,
        chart_viz,
        chart_created_on,
        dashboard_title,
        dashboard_created_on,
        ROW_NUMBER() over (
            PARTITION BY action_id
        ) AS row_no
    FROM
        {{ ref('actions') }}
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
        chart_title,
        chart_viz,
        chart_created_on,
        CASE
            WHEN dashboard_title IS NOT NULL THEN 'All'
            ELSE dashboard_title
        END AS dashboard_title,
        dashboard_created_on,
        ROW_NUMBER() over (
            PARTITION BY action_id
        ) AS row_no
    FROM
        {{ ref('actions') }}
),
actions_all AS (
    SELECT
        action_id,
        user_id,
        user_name,
        user_created_on,
        CASE
            WHEN role_name IS NOT NULL THEN 'All'
            ELSE role_name
        END AS role_name,
        action,
        action_count,
        action_date,
        chart_title,
        chart_viz,
        chart_created_on,
        CASE
            WHEN dashboard_title IS NOT NULL THEN 'All'
            ELSE dashboard_title
        END AS dashboard_title,
        dashboard_created_on,
        ROW_NUMBER() over (
            PARTITION BY action_id
        ) AS row_no
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
        chart_title,
        chart_viz,
        chart_created_on,
        dashboard_title,
        dashboard_created_on,
        1 AS row_no
    FROM
        {{ ref('actions') }}
    UNION ALL
    SELECT
        *
    FROM
        actions_all_roles
    WHERE
        row_no = 1
    UNION ALL
    SELECT
        *
    FROM
        actions_all_dashboards
    WHERE
        row_no = 1
    UNION ALL
    SELECT
        *
    FROM
        actions_all
    WHERE
        row_no = 1
),
user_action_counts AS (
    SELECT
        params.role_name,
        params.dashboard_title,
        params.month_start_date,
        params.month_end_date,
        actions.user_id,
        actions.user_name,
        SUM(action_count) AS action_count
    FROM
        params
        LEFT JOIN actions
        ON params.role_name = actions.role_name
        AND params.dashboard_title = actions.dashboard_title
        AND actions.action_date >= params.month_start_date
        AND actions.action_date <= params.month_end_date
    GROUP BY
        params.role_name,
        params.dashboard_title,
        params.month_start_date,
        params.month_end_date,
        actions.user_id,
        actions.user_name
)
SELECT
    *
FROM
    user_action_counts
