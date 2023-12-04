{{ config(
    materialized = "table",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        '{{ org }}' AS org

        {% if not loop.last -%}
    UNION ALL
    {%- endif %}
{% endfor %}
