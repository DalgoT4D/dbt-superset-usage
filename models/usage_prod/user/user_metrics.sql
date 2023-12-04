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
                "name" AS role_name
            FROM
                {{ ref('roles') }}
            UNION
            SELECT
                'All' AS role_name
        ) roles
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
        ) AS row_no,
        org
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
        ) AS row_no,
        org
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
        chart_title,
        chart_viz,
        chart_created_on,
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
new_users AS (
    SELECT
        params.role_name,
        params.org,
        params.dashboard_title,
        params.month_start_date,
        params.month_end_date,
        COUNT(
            DISTINCT actions.user_id
        ) AS new_users
    FROM
        params
        LEFT JOIN actions
        ON params.role_name = actions.role_name
        AND params.dashboard_title = actions.dashboard_title
        AND actions.user_created_on <= params.month_end_date
        AND params.org = actions.org
    GROUP BY
        params.role_name,
        params.org,
        params.dashboard_title,
        params.month_start_date,
        params.month_end_date
),
user_action_counts AS (
    SELECT
        params.role_name,
        params.org,
        params.dashboard_title,
        params.month_start_date,
        params.month_end_date,
        actions.user_id,
        SUM(action_count) AS action_count
    FROM
        params
        LEFT JOIN actions
        ON params.role_name = actions.role_name
        AND params.dashboard_title = actions.dashboard_title
        AND actions.action_date >= params.month_start_date
        AND actions.action_date <= params.month_end_date
        AND actions.org = params.org
    GROUP BY
        params.role_name,
        params.org,
        params.dashboard_title,
        params.month_start_date,
        params.month_end_date,
        actions.user_id
),
active_users AS (
    SELECT
        user_action_counts.role_name,
        user_action_counts.org,
        user_action_counts.dashboard_title,
        user_action_counts.month_start_date,
        user_action_counts.month_end_date,
        COUNT(
            DISTINCT user_id
        ) AS active_users
    FROM
        user_action_counts
    WHERE
        action_count > 0
    GROUP BY
        user_action_counts.role_name,
        user_action_counts.org,
        user_action_counts.dashboard_title,
        user_action_counts.month_start_date,
        user_action_counts.month_end_date
)
SELECT
    new_users.role_name,
    new_users.org,
    new_users.dashboard_title,
    new_users.month_start_date,
    new_users.month_end_date,
    new_users.new_users,
    CASE
        WHEN active_users.active_users IS NULL THEN 0
        ELSE active_users.active_users
    END
FROM
    new_users
    LEFT JOIN active_users
    ON new_users.role_name = active_users.role_name
    AND new_users.dashboard_title = active_users.dashboard_title
    AND new_users.month_start_date = active_users.month_start_date
    AND new_users.month_end_date = active_users.month_end_date
    AND new_users.org = active_users.org
