{{ config(
    materialized = "table",
    schema = "usage_int"
) }}

WITH org_start_end_dates AS (

    SELECT
        org,
        DATE_TRUNC('month', MIN(action_date) - INTERVAL '1 month') AS org_start_date,
        DATE_TRUNC('month', MAX(action_date) + INTERVAL '1 month') AS org_end_date
    FROM
        {{ ref('logs') }}
    GROUP BY
        org
),
create_date_series AS (
    {{ dbt_utils.date_spine(
        datepart = "month",
        start_date = "(select cast(date_trunc('month', min(action_date)) as date) from" + ref('logs') | string + ")",
        end_date = "(select cast(date_trunc('month', max(action_date)) as date) + INTERVAL '1 MONTH' from" + ref('logs') | string + ")"
    ) }}
),
get_month_end_date AS (
    SELECT
        (
            date_month + INTERVAL '1 month' - INTERVAL '1 day'
        ) :: DATE AS month_end_date
    FROM
        create_date_series
),
get_all_possible_monthly_date_ranges AS (
    SELECT
        date_month :: DATE AS month_start_date,
        month_end_date
    FROM
        create_date_series
        CROSS JOIN get_month_end_date
    WHERE
        EXTRACT(
            days
            FROM
                month_end_date - date_month
        ) :: INT < 32
        AND date_month < month_end_date
),
month_start_end_params AS (
    SELECT
        *
    FROM
        get_all_possible_monthly_date_ranges
        CROSS JOIN org_start_end_dates
    WHERE
        (
            month_start_date BETWEEN org_start_date
            AND org_end_date
        )
        AND (
            month_end_date BETWEEN org_start_date
            AND org_end_date
        )
),
org_dashboard_params AS (
    SELECT
        dashboards.org,
        dashboards.id,
        dashboards.dashboard_title
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
        dashboards.dashboard_title
    UNION ALL
    SELECT
        org,
        NULL,
        'All'
    FROM
        {{ ref('dashboards') }}
    WHERE
        published IS TRUE
    GROUP BY
        org
),
role_params AS (
    SELECT
        user_roles.org,
        user_roles.role_id,
        roles.name AS role_name
    FROM
        {{ ref('user_roles') }} AS user_roles
        INNER JOIN {{ ref('roles') }} AS roles
        ON user_roles.role_id = roles.id
        ON user_roles.org = roles.org
    GROUP BY
        user_roles.org,
        user_roles.role_id,
        roles.name
    UNION ALL
    SELECT
        org,
        NULL,
        'All'
    FROM
        {{ ref('user_roles') }}
    GROUP BY
        org
)
SELECT
    month_param.org,
    month_param.month_start_date,
    month_param.month_end_date,
    dash_param.id AS dashboard_id,
    dash_param.dashboard_title,
    role_param.role_id,
    role_param.role_name
FROM
    month_start_end_params AS month_param
    INNER JOIN org_dashboard_params AS dash_param
    ON month_param.org = dash_param.org
    INNER JOIN role_params AS role_param
    ON month_param.org = role_param.org
