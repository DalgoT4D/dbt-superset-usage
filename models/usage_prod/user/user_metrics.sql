{{ config(
    materialized = "table",
    schema = "usage_prod"
) }}

WITH metrics AS (

    SELECT
        monthly_actions.role_name,
        monthly_actions.org,
        monthly_actions.dashboard_title,
        monthly_actions.month_start_date,
        monthly_actions.month_end_date,
        COUNT(
            monthly_actions.user_id
        ) AS total_users,
        SUM(
            monthly_actions.action_count
        ) AS total_visits,
        SUM(
            CASE
                WHEN is_active = 'yes' THEN 1
                ELSE 0
            END
        ) AS active_users
    FROM
        {{ ref('monthly_actions') }}
        monthly_actions
    GROUP BY
        monthly_actions.role_name,
        monthly_actions.org,
        monthly_actions.dashboard_title,
        monthly_actions.month_start_date,
        monthly_actions.month_end_date
)
SELECT
    metrics.org,
    metrics.role_name,
    metrics.dashboard_title,
    metrics.month_start_date,
    metrics.month_end_date,
    metrics.total_users,
    metrics.total_visits,
    metrics.active_users,
    COALESCE(
        ROUND(
            100 * {{ dbt_utils.safe_divide(
                'active_users',
                'total_users'
            ) }},
            2
        ),
        0
    ) AS active_over_total_ratio,
    COALESCE(
        ROUND(
            {{ dbt_utils.safe_divide(
                'total_visits',
                'active_users'
            ) }},
            2
        ),
        0
    ) AS visits_per_active_user
FROM
    metrics
