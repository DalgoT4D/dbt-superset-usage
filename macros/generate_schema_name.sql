{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%} {{ default_schema }}_{{env_var('DBT_USAGE_VER') }}

    {%- else -%} {{ custom_schema_name }}_{{env_var('DBT_USAGE_VER')}}

    {%- endif -%}

{%- endmacro %}
