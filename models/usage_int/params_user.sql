{{ config(
    materialized = "table",
    schema = "usage_int"
) }}

WITH dashboard_user_role AS (

    SELECT
        dashboards.org,
        dashboards.id AS dashboard_id,
        dashboards.dashboard_title,
        dashboard_roles.role_id,
        user_roles.role_name,
        user_roles.user_id,
        user_roles.user_name,
        user_roles.user_created_on
    FROM
        {{ ref('dashboards') }}
        dashboards
        INNER JOIN {{ ref('dashboard_roles') }}
        dashboard_roles
        ON dashboards.id = dashboard_roles.dashboard_id
        AND dashboards.org = dashboard_roles.org
        INNER JOIN {{ ref('userroles') }}
        user_roles
        ON user_roles.role_id = dashboard_roles.role_id
        AND user_roles.org = dashboard_roles.org
),
dashboard_user_role_all AS (
    SELECT
        *
    FROM
        dashboard_user_role
    UNION ALL
    SELECT
        dashboard_user_role.org,
        NULL AS dashboard_id,
        'All' AS dashboard_title,
        dashboard_user_role.role_id,
        dashboard_user_role.role_name,
        dashboard_user_role.user_id,
        dashboard_user_role.user_name,
        dashboard_user_role.user_created_on
    FROM
        dashboard_user_role
    GROUP BY
        dashboard_user_role.org,
        dashboard_user_role.role_id,
        dashboard_user_role.role_name,
        dashboard_user_role.user_id,
        dashboard_user_role.user_name,
        dashboard_user_role.user_created_on
    UNION ALL
    SELECT
        dashboard_user_role.org,
        dashboard_user_role.dashboard_id,
        dashboard_user_role.dashboard_title,
        NULL AS role_id,
        'All' AS role_name,
        dashboard_user_role.user_id,
        dashboard_user_role.user_name,
        dashboard_user_role.user_created_on
    FROM
        dashboard_user_role
    GROUP BY
        dashboard_user_role.org,
        dashboard_user_role.dashboard_id,
        dashboard_user_role.dashboard_title,
        dashboard_user_role.user_id,
        dashboard_user_role.user_name,
        dashboard_user_role.user_created_on
    UNION ALL
    SELECT
        dashboard_user_role.org,
        NULL AS dashboard_id,
        'All' AS dashboard_title,
        NULL AS role_id,
        'All' AS role_name,
        dashboard_user_role.user_id,
        dashboard_user_role.user_name,
        dashboard_user_role.user_created_on
    FROM
        dashboard_user_role
    GROUP BY
        dashboard_user_role.org,
        dashboard_user_role.user_id,
        dashboard_user_role.user_name,
        dashboard_user_role.user_created_on
)
SELECT
    params.month_start_date,
    params.month_end_date,
    dashboard_user_role_all.*
FROM
    dashboard_user_role_all
    INNER JOIN {{ ref('params') }}
    params
    ON dashboard_user_role_all.org = params.org
