{{ config(
    materialized = "incremental",
    schema = "usage_staging"
) }}

{% for org in fetch_org_names() %}

    SELECT
        *,
        dttm AS action_date,
        "json" :: json ->> 'class_name' AS resource_name,
        (
            "json" :: json ->> 'obj_id'
        ) :: INT AS resource_id,
        '{{ org }}' AS org
    FROM
        {{ source(
            org,
            'logs'
        ) }}
    WHERE
        action IN (
            'DashboardRestApi.get' -- consider only the dashboard visits
        )

{% if is_incremental() %}
AND dttm > (
    SELECT
        MAX(dttm)
    FROM
        {{ this }}
)
{% endif %}

{% if not loop.last -%}
UNION ALL
{%- endif %}
{% endfor %}
