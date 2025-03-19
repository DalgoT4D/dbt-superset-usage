{{ config(
    materialized = "table",
    schema = "usage_int"
) }}

WITH create_date_series AS (
    {{ dbt_utils.date_spine(
        datepart = "month",
        start_date = "(select cast(date_trunc('month', min(action_date)) as date) from" + ref('logs') | string + ")",
        end_date = "(select cast(date_trunc('month', max(action_date)) as date) + INTERVAL '1 MONTH' from" + ref('logs') | string + ")"
    ) }}
),
org_start_end_dates AS (
    SELECT
        org,
        DATE_TRUNC('month', MIN(action_date) - INTERVAL '1 month') AS org_start_date,
        DATE_TRUNC('month', MAX(action_date) + INTERVAL '1 month') AS org_end_date
    FROM
        {{ ref('logs') }}
    GROUP BY
        org
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
)
SELECT
    month_param.org,
    month_param.month_start_date,
    month_param.month_end_date
FROM
    month_start_end_params AS month_param
