{{ config(
    materialized = "table",
    schema = "usage_prod"
) }}

WITH actions_all_roles AS (

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
total_users AS (
    SELECT
        params.role_name,
        params.org,
        params.dashboard_title,
        params.month_start_date,
        params.month_end_date,
        COUNT(
            DISTINCT actions.user_id
        ) AS total_users,
        SUM(
            actions.action_count
        ) AS total_visits
    FROM
        {{ ref('params') }} AS params
        LEFT JOIN actions
        ON params.org = actions.org
        AND params.role_name = actions.role_name
        AND params.dashboard_title = actions.dashboard_title
        AND actions.user_created_on <= params.month_end_date
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
        {{ ref('params') }} AS params
        LEFT JOIN actions
        ON actions.org = params.org
        AND params.role_name = actions.role_name
        AND params.dashboard_title = actions.dashboard_title
        AND actions.action_date >= params.month_start_date
        AND actions.action_date <= params.month_end_date
    GROUP BY
        params.org,
        params.role_name,
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
        user_action_counts.org,
        user_action_counts.role_name,
        user_action_counts.dashboard_title,
        user_action_counts.month_start_date,
        user_action_counts.month_end_date
)
SELECT
    total_users.org,
    total_users.role_name,
    total_users.dashboard_title,
    total_users.month_start_date,
    total_users.month_end_date,
    total_users.total_users,
    total_users.total_visits,
    CASE
        WHEN active_users.active_users IS NULL THEN 0
        ELSE active_users.active_users
    END AS active_users,
    COALESCE(
        100 * {{ dbt_utils.safe_divide(
            'active_users',
            'total_users'
        ) }},
        0
    ) AS active_over_total_ratio,
    COALESCE(
        {{ dbt_utils.safe_divide(
            'total_visits',
            'active_users'
        ) }},
        0
    ) AS visits_per_active_user
FROM
    total_users
    LEFT JOIN active_users
    ON total_users.org = active_users.org
    AND total_users.role_name = active_users.role_name
    AND total_users.dashboard_title = active_users.dashboard_title
    AND total_users.month_start_date = active_users.month_start_date
    AND total_users.month_end_date = active_users.month_end_date
