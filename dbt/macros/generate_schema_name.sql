{# Use +schema as the Unity Catalog schema name (matches PySpark silver/gold tables). #}
{# This macro ensures that dbt always writes to the same schema as PySpark. #}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none and custom_schema_name | trim != '' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
