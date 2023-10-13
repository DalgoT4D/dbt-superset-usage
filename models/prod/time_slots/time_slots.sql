{{ config(
    materialized = "table",
    schema = "prod"
) }}

WITH create_date_series AS (
    {{ dbt_utils.date_spine(
        datepart = "month",
        start_date = "(select cast(date_trunc('month', min(action_date)) as date) from" + ref('actions') | string + ")",
        end_date = "(select cast(date_trunc('month', max(action_date)) as date) + INTERVAL '1 MONTH' from" + ref('actions') | string + ")",
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
        date_month < month_end_date
)
SELECT
    *
FROM
    get_all_possible_monthly_date_ranges
