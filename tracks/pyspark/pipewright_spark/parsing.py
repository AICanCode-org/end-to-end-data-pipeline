"""Column expressions for contract/transform-spec.md §2 (the same rules as the dbt macros in tracks/sql/macros)."""

from __future__ import annotations

from pyspark.sql import Column


def utc_timestamp(col: Column) -> Column:
    """ISO-8601 text with an offset ('Z' or '+HH:MM') -> UTC TIMESTAMP_NTZ."""
    # TODO(Phase 3 M2): implement utc_timestamp.
    # The first 19 characters are local time; the rest is 'Z' or +HH:MM / -HH:MM. Subtract the offset
    # (F.make_dt_interval) to get a UTC TIMESTAMP_NTZ. Same cases as tracks/sql/tests/singular/parsing_macros.sql.
    raise NotImplementedError("Phase 3 M2: utc_timestamp (see the TODO above)")


def ist_date(ts: Column) -> Column:
    """The IST calendar date of a UTC timestamp (IST = UTC + 5:30)."""
    # TODO(Phase 3 M2): implement ist_date.
    # The IST calendar date of a UTC timestamp: add 330 minutes, then F.to_date.
    raise NotImplementedError("Phase 3 M2: ist_date (see the TODO above)")


def to_paise(col: Column) -> Column:
    """Rupees as text ('₹12,345.50', '12,345.5', '499.00') -> integer paise, without floating point."""
    # TODO(Phase 3 M2): implement to_paise.
    # Remove '₹', ',' and spaces; whole rupees * 100 + the fraction padded to 2 digits. No floats, ever.
    raise NotImplementedError("Phase 3 M2: to_paise (see the TODO above)")


def clean_key(col: Column) -> Column:
    """lower(trim(x)): how raw spellings are matched against the city map."""
    # TODO(Phase 3 M2): implement clean_key.
    # lower(trim(x)).
    raise NotImplementedError("Phase 3 M2: clean_key (see the TODO above)")
