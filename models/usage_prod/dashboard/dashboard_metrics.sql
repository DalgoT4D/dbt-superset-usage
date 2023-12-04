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
        dashboard_title,
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
)
SELECT
    params.role_name,
    params.month_start_date,
    params.month_end_date,
    params.org,
    COUNT(
        DISTINCT actions.dashboard_title
    ) AS active_dashboards
FROM
    params
    LEFT JOIN (
        SELECT
            *
        FROM
            actions
        WHERE
            dashboard_title IS NOT NULL
            AND action = 'DashboardRestApi.get' -- consider only the visits of dashbords
    ) actions
    ON params.role_name = actions.role_name
    AND actions.action_date >= params.month_start_date
    AND actions.action_date <= params.month_end_date
GROUP BY
    params.role_name,
    params.month_start_date,
    params.month_end_date,
    params.org
