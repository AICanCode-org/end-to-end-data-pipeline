-- noqa: disable=AL03
-- The parsing macros against hand-worked cases (contract/transform-spec.md §2). Any returned row is a bug.
with cases as (
    select
        '2026-06-01T09:39:12+05:30' as text_value,
        timestamp '2026-06-01 04:09:12' as want_utc,
        date '2026-06-01' as want_ist_date
    union all
    select '2026-06-01T04:09:12Z', timestamp '2026-06-01 04:09:12', date '2026-06-01'
    union all
    select '2026-06-01T19:00:00Z', timestamp '2026-06-01 19:00:00', date '2026-06-02'
    union all
    select '2026-06-02T00:30:00+05:30', timestamp '2026-06-01 19:00:00', date '2026-06-02'
    union all
    select '2026-06-01T10:00:00-02:00', timestamp '2026-06-01 12:00:00', date '2026-06-01'
),

money as (
    select
        '499.00' as text_value,
        49900 as want_paise
    union all
    select '₹12,345.50', 1234550
    union all
    select '12,345.5', 1234550
    union all
    select '12,340', 1234000
    union all
    select ' 7 ', 700
)

select
    text_value,
    'timestamp' as kind
from cases
where {{ utc_timestamp('text_value') }} != want_utc or {{ ist_date(utc_timestamp('text_value')) }} != want_ist_date
union all
select
    text_value,
    'money' as kind
from money
where {{ to_paise('text_value') }} != want_paise
