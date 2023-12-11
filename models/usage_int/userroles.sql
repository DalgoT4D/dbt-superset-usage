{{ config(
    materialized = "table",
    schema = "usage_int"
) }}

WITH cte_user_roles AS (

    SELECT
        user_roles.org,
        user_roles.user_id,
        user_roles.role_id,
        roles.name AS role_name
    FROM
        {{ ref('user_roles') }} AS user_roles
        INNER JOIN {{ ref('roles') }} AS roles
        ON user_roles.role_id = roles.id
        AND user_roles.org = roles.org
)
SELECT
    id AS user_id,
    users.org,
    CONCAT(
        first_name,
        ' ',
        last_name
    ) AS user_name,
    cte_user_roles.role_name,
    cte_user_roles.role_id,
    users.created_on AS user_created_on
FROM
    {{ ref("users") }} AS users
    INNER JOIN cte_user_roles
    ON cte_user_roles.user_id = users.id
    AND cte_user_roles.org = users.org
