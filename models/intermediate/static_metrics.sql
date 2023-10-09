{{ config(
    materialized = "table",
    schema = "intermediate"
) }}

SELECT
    'user' AS TYPE,
    COUNT(
        DISTINCT user_id
    ) AS total
FROM
    {{ ref(
        'user_roles'
    ) }}
UNION
SELECT
    'dashboard' AS TYPE,
    COUNT(*) AS total
FROM
    {{ source(
        'superset',
        'dashboards'
    ) }}
WHERE
    dashboard_title IS NOT NULL
UNION
SELECT
    'chart' AS TYPE,
    COUNT(*) AS total
FROM
    {{ source(
        'superset',
        'slices'
    ) }}
