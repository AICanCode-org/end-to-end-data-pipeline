{#- contract/transform-spec.md §2: ISO-8601 text with an offset ('Z' or '+HH:MM') -> UTC TIMESTAMP. -#}
{% macro utc_timestamp(col) -%}
    (
        strptime(substr({{ col }}, 1, 19), '%Y-%m-%dT%H:%M:%S')
        - to_minutes(
            case
                when {{ col }} like '%Z' then 0
                else
                    (case when substr({{ col }}, 20, 1) = '-' then -1 else 1 end)
                    * (cast(substr({{ col }}, 21, 2) as integer) * 60 + cast(substr({{ col }}, 24, 2) as integer))
            end
        )
    )
{%- endmacro %}

{#- §2: the IST calendar date of a UTC timestamp (IST = UTC + 5:30, no daylight saving). -#}
{% macro ist_date(ts) -%}
    cast({{ ts }} + interval 330 minute as date)
{%- endmacro %}

{#- §2: rupees written as text ('₹12,345.50', '12,345.5', '499.00') -> integer paise, exactly (no floats). -#}
{% macro to_paise(col) -%}
    (
        cast(split_part(replace(replace(replace(trim({{ col }}), '₹', ''), ',', ''), ' ', ''), '.', 1) as bigint) * 100
        + cast(
            rpad(
                coalesce(nullif(split_part(replace(replace(replace(trim({{ col }}), '₹', ''), ',', ''), ' ', ''), '.', 2), ''), '0'),
                2,
                '0'
            ) as bigint
        )
    )
{%- endmacro %}
