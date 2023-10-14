{{ config(
    materialized = "table",
    schema = "intermediate"
) }}

WITH cte_dashboards AS (

    SELECT
        dashboards.id,
        dashboards.dashboard_title,
        dashboards.created_on
    FROM
        {{ source(
            'superset',
            'dashboards'
        ) }}
        dashboards
        INNER JOIN {{ source(
            'superset',
            'dashboard_roles'
        ) }}
        dashboard_roles
        ON dashboards.id = dashboard_roles.dashboard_id
    WHERE
        dashboards.published IS TRUE
    GROUP BY
        dashboards.id,
        dashboards.dashboard_title,
        dashboards.created_on
),
cte_slices AS (
    SELECT
        id,
        slice_name,
        viz_type,
        created_on
    FROM
        {{ source(
            'superset',
            'slices'
        ) }}
)
SELECT
    logs.id AS action_id,
    user_roles.user_id,
    user_roles.user_name,
    user_roles.created_on AS user_created_on,
    user_roles.role AS role_name,
    logs.action,
    1 AS action_count,
    logs.dttm AS action_date,
    cte_slices.slice_name AS chart_title,
    cte_slices.viz_type AS chart_viz,
    cte_slices.created_on AS chart_created_on,
    cte_dashboards.dashboard_title,
    cte_dashboards.created_on AS dashboard_created_on
FROM
    {{ ref('user_roles') }} AS user_roles
    LEFT JOIN (
        SELECT
            *
        FROM
            {{ source(
                'superset',
                'logs'
            ) }}
        WHERE
            dashboard_id IS NULL
            AND slice_id IS NULL
        UNION ALL
        SELECT
            logs.*
        FROM
            {{ source(
                'superset',
                'logs'
            ) }}
            logs
            INNER JOIN cte_dashboards
            ON cte_dashboards.id = logs.dashboard_id
        UNION ALL
        SELECT
            *
        FROM
            {{ source(
                'superset',
                'logs'
            ) }}
        WHERE
            slice_id IS NOT NULL
            AND dashboard_id IS NULL
    ) AS logs
    ON user_roles.user_id = logs.user_id
    LEFT JOIN cte_dashboards
    ON cte_dashboards.id = logs.dashboard_id
    LEFT JOIN cte_slices
    ON cte_slices.id = logs.slice_id
WHERE
    logs.action IN (
        'DashboardRestApi.get',
        'DashboardRestApi.delete',
        'DashboardRestApi.get_charts',
        'DashboardRestApi.put',
        'DashboardRestApi.info',
        'DashboardRestApi.get_charts',
        'ChartRestApi.post',
        'ChartRestApi.bulk_delete',
        'ChartRestApi.get_list',
        'ChartRestApi.put',
        'ChartRestApi.delete',
        'ChartRestApi.get',
        'ChartRestApi.info',
        'DashboardFilterStateRestApi.post',
        'DashboardFilterStateRestApi.get',
        'DashboardFilterStateRestApi.put'
    )
