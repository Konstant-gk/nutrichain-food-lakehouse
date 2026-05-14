{# generate_schema_name.sql
   dbt default: {target.schema}_{custom_schema} → e.g. gold + gold = gold_gold (wrong for UC).
   NutriChain: PySpark jobs write nutrichain_lakehouse.silver / .gold; use the configured
   +schema value as the literal UC schema name so dbt lands in the same schemas.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none and custom_schema_name | trim != '' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
