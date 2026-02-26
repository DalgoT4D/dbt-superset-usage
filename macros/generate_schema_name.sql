{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%} {{ default_schema }}_v1

    {%- else -%} {{ custom_schema_name }}_v1

    {%- endif -%}

{%- endmacro %}
