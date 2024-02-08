{{ config(
    materialized = "table",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        role_id,
        user_id,
        id,
        '{{ org }}' AS org
    FROM
        {{ source(
            org,
            'user_roles'
        ) }}

        {% if not loop.last -%}
        UNION ALL
        {%- endif %}
    {% endfor %}
