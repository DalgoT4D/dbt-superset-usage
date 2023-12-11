{{ config(
    materialized = "table",
    schema = "usage_int"
) }}

SELECT
    params.*,
    users.user_id,
    users.user_name,
    users.user_created_on
FROM
    {{ ref('params') }} AS params
    CROSS JOIN (
        SELECT
            user_id,
            user_name,
            user_created_on,
            role_id,
            org
        FROM
            {{ ref('userroles') }} AS userroles
    ) AS users
WHERE
    params.org = users.org
    AND params.role_id = users.role_id
    AND params.role_name != 'All'
UNION ALL
SELECT
    params.*,
    users.user_id,
    users.user_name,
    users.user_created_on
FROM
    {{ ref('params') }} AS params
    LEFT JOIN (
        SELECT
            user_id,
            user_name,
            user_created_on,
            role_id,
            org
        FROM
            {{ ref('userroles') }} AS userroles
    ) AS users
    ON params.org = users.org
WHERE
    params.role_name = 'All'
