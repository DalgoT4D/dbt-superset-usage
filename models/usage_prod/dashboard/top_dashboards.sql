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
        dashboard_title AS dashboard_name,
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
        dashboard_title AS dashboard_name,
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
        dashboard_title AS dashboard_name,
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
        dashboard_title AS dashboard_name,
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
top_dashboards AS (
    SELECT
        params.role_name,
        params.month_start_date,
        params.month_end_date,
        params.org,
        actions.dashboard_title,
        actions.dashboard_name,
        SUM(action_count) AS total_visits,
        MAX(action_date) AS last_visited_at
    FROM
        {{ ref('params_user') }} AS params
        LEFT JOIN actions
        ON params.role_name = actions.role_name
        AND params.org = actions.org
        AND params.dashboard_title = actions.dashboard_title
        AND actions.user_id = params.user_id
        AND actions.action_date >= params.month_start_date
        AND actions.action_date <= params.month_end_date
    GROUP BY
        params.role_name,
        params.month_start_date,
        params.month_end_date,
        params.org,
        actions.dashboard_title,
        actions.dashboard_name
),
top_dashboards_last_visited_by AS (
    SELECT 
        params.role_name,
        params.month_start_date,
        params.month_end_date,
        params.org,
        actions.dashboard_title,
        actions.dashboard_name,
        actions.user_name,
        ROW_NUMBER() OVER (
            PARTITION BY params.role_name, params.month_start_date, params.month_end_date, params.org, actions.dashboard_title, actions.dashboard_name
            ORDER BY actions.action_date DESC
        ) AS row_no 
    FROM {{ ref('params_user') }} AS params
        LEFT JOIN actions
        ON params.role_name = actions.role_name
        AND params.org = actions.org
        AND params.dashboard_title = actions.dashboard_title
        AND actions.user_id = params.user_id
        AND actions.action_date >= params.month_start_date
        AND actions.action_date <= params.month_end_date
)
SELECT 
    td.*, 
    td_lvb.user_name as last_visited_by
FROM top_dashboards AS td
INNER JOIN (
    SELECT *
    FROM top_dashboards_last_visited_by
    WHERE row_no = 1
) AS td_lvb 
  ON td.role_name = td_lvb.role_name AND
    td.month_start_date = td_lvb.month_start_date AND
    td.month_end_date = td_lvb.month_end_date AND
    td.org = td_lvb.org AND
    td.dashboard_title = td_lvb.dashboard_title AND
    td.dashboard_name = td_lvb.dashboard_name