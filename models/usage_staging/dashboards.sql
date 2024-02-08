{{ config(
    materialized = "table",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        created_by_fk,
        css,
        description,
        certified_by,
        published,
        uuid,
        external_url,
        dashboard_title,
        created_on,
        position_json,
        is_managed_externally,
        id,
        slug,
        changed_on,
        json_metadata,
        certification_details,
        '{{ org }}' AS org
    FROM
        {{ source(
            org,
            'dashboards'
        ) }}

        {% if not loop.last -%}
        UNION ALL
        {%- endif %}
    {% endfor %}
