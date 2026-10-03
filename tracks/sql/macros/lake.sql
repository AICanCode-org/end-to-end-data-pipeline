{#- Where the bronze files are. The runner passes an absolute PIPEWRIGHT_LAKE_DIR. -#}
{% macro lake_glob(source, ext) -%}
    {{ env_var('PIPEWRIGHT_LAKE_DIR', '../../lake') }}/raw/{{ source }}/dt=*/*.{{ ext }}
{%- endmacro %}

{#- The business date being built, as a DATE literal. -#}
{% macro run_date() -%}
    cast('{{ var("run_date") }}' as date)
{%- endmacro %}

{#- Use the custom schema name as is (silver, gold) instead of dbt's default <target>_<custom>. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
