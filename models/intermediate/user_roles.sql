{{ config(
    materialized = "table",
    schema = "intermediate"
) }}

WITH cte_user_roles AS (

    SELECT
        user_roles.user_id,
        user_roles.role_id,
        roles.name AS role
    FROM
        {{ source(
            'superset',
            'user_roles'
        ) }} AS user_roles
        INNER JOIN {{ source(
            'superset',
            'roles'
        ) }} AS roles
        ON user_roles.role_id = roles.id
)
SELECT
    id AS user_id,
    CONCAT(
        first_name,
        ' ',
        last_name
    ) AS user_name,
    cte_user_roles.role,
    created_on
FROM
    {{ source(
        "superset",
        "users"
    ) }} AS users
    LEFT JOIN cte_user_roles
    ON cte_user_roles.user_id = users.id
WHERE
    first_name != ''
    AND last_name != ''
