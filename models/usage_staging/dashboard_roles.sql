{{ config(
    materialized = "table",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        role_id,
        id,
        dashboard_id,
        '{{ org }}' AS org
    FROM
        {{ source(
            org,
            'dashboard_roles'
        ) }}

        {% if not loop.last -%}
        UNION ALL
        {%- endif %}
    {% endfor %}
