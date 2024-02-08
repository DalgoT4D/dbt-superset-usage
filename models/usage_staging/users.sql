{{ config(
    materialized = "table",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        created_by_fk,
        last_login,
        active,
        last_name,
        fail_login_count,
        login_count,
        password,
        blob,
        created_on,
        changed_by_fk,
        id,
        first_name,
        email,
        changed_on,
        username,
        '{{ org }}' AS org
    FROM
        {{ source(
            org,
            'users'
        ) }}

        {% if not loop.last -%}
        UNION ALL
        {%- endif %}
    {% endfor %}
