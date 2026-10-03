{#- contract/transform-spec.md §2: ISO-8601 text with an offset ('Z' or '+HH:MM') -> UTC TIMESTAMP. -#}
{#- TODO(Phase 3 M2): implement. The first 19 characters are local time; subtract the offset (to_minutes()). -#}
{% macro utc_timestamp(col) -%}
    cast(null as timestamp)
{%- endmacro %}

{#- §2: the IST calendar date of a UTC timestamp (IST = UTC + 5:30, no daylight saving). -#}
{#- TODO(Phase 3 M2): implement (add 330 minutes, then cast to date). -#}
{% macro ist_date(ts) -%}
    cast(null as date)
{%- endmacro %}

{#- §2: rupees written as text ('₹12,345.50', '12,345.5', '499.00') -> integer paise, exactly (no floats). -#}
{#- TODO(Phase 3 M2): implement (strip '₹', ',' and spaces; whole * 100 + the fraction padded to 2 digits). -#}
{% macro to_paise(col) -%}
    cast(null as bigint)
{%- endmacro %}
