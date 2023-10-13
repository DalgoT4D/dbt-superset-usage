{{ config(
    materialized = "table",
    schema = "intermediate"
) }}

WITH cte_dashboards AS (

    SELECT
        id,
        dashboard_title,
        published,
        created_on
    FROM
        {{ source(
            'superset',
            'dashboards'
        ) }}
    WHERE
        dashboard_title IS NOT NULL
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
    user_roles.id AS user_id,
    CONCAT(
        user_roles.first_name,
        ' ',
        user_roles.last_name
    ) AS user_name,
    user_roles.created_on AS user_created_on,
    'All' AS role_name,
    logs.action,
    1 AS action_count,
    logs.dttm AS action_date,
    logs.slice_id AS chart_id,
    cte_slices.slice_name AS chart_title,
    cte_slices.viz_type AS chart_viz,
    cte_slices.created_on AS chart_created_on,
    cte_dashboards.id AS dashboard_id,
    cte_dashboards.dashboard_title,
    cte_dashboards.created_on AS dashboard_created_on,
    CASE
        WHEN cte_dashboards.published THEN 'published'
        ELSE 'draft'
    END AS dashboard_status
FROM
    {{ source(
        'superset',
        'users'
    ) }} AS user_roles
    LEFT JOIN {{ source(
        'superset',
        'logs'
    ) }} AS logs
    ON user_roles.id = logs.user_id
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