{{ config(
    materialized = "table",
    schema = "usage_int"
) }}

WITH cte_dashboards AS (

    SELECT
        dashboards.org,
        dashboards.id,
        dashboards.dashboard_title,
        dashboards.created_on
    FROM
        {{ ref('dashboards') }}
        dashboards
        INNER JOIN {{ ref('dashboard_roles') }}
        dashboard_roles
        ON dashboards.id = dashboard_roles.dashboard_id
        AND dashboards.org = dashboard_roles.org
    WHERE
        dashboards.published IS TRUE
    GROUP BY
        dashboards.org,
        dashboards.id,
        dashboards.dashboard_title,
        dashboards.created_on
)
SELECT
    logs.id AS action_id,
    user_roles.user_id,
    user_roles.user_name,
    user_roles.user_created_on,
    user_roles.role_name,
    user_roles.org,
    logs.action,
    1 AS action_count,
    action_date,
    cte_dashboards.dashboard_title,
    cte_dashboards.created_on AS dashboard_created_on
FROM
    {{ ref('userroles') }} AS user_roles
    LEFT JOIN (
        SELECT
            logs.*
        FROM
            {{ ref('logs') }}
            logs
            INNER JOIN cte_dashboards
            ON cte_dashboards.id = logs.dashboard_id
            AND cte_dashboards.org = logs.org
    ) AS logs
    ON user_roles.user_id = logs.user_id
    AND user_roles.org = logs.org
    LEFT JOIN cte_dashboards
    ON cte_dashboards.id = logs.dashboard_id
    AND cte_dashboards.org = logs.org
WHERE
    logs.action IN (
        'DashboardRestApi.get' -- consider only the dashboard visits
    )
