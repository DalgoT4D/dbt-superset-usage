{{ config(
    materialized = "incremental",
    schema = "usage_staging"
) }}


{% for org in fetch_org_names() %}

    SELECT
        duration_ms,
        referrer,
        user_id,
        dttm,
        json,
        action,
        id,
        slice_id,
        dashboard_id,
        dttm AS action_date,
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
    {% if org != var('full_refresh_logs_for_org', 'default_org') %}
    AND dttm > (
        SELECT
            MAX(dttm)
        FROM
            {{ this }}
        WHERE
            org = '{{ org }}'
    )
    {% endif %}
{% endif %}

{% if not loop.last -%}
UNION ALL
{%- endif %}
{% endfor %}
