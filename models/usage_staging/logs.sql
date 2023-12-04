{{ config(
    materialized = "table",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        *,
        dttm AS action_date,
        '{{ org }}' AS org
    FROM
        {{ source(
            org,
            'logs'
        ) }}

        {% if not loop.last -%}
        UNION ALL
        {%- endif %}
    {% endfor %}
