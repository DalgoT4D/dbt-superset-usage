{{ config(
    materialized = "table",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        created_by_fk,
        datasource_id,
        perm,
        cache_timeout,
        last_saved_at,
        description,
        certified_by,
        viz_type,
        params,
        query_context,
        uuid,
        schema_perm,
        slice_name,
        external_url,
        created_on,
        datasource_type,
        changed_by_fk,
        last_saved_by_fk,
        is_managed_externally,
        datasource_name,
        id,
        changed_on,
        certification_details,
        '{{ org }}' AS org
    FROM
        {{ source(
            org,
            'slices'
        ) }}

        {% if not loop.last -%}
        UNION ALL
        {%- endif %}
    {% endfor %}
