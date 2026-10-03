"""Column expressions for contract/transform-spec.md §2 (the same rules as the dbt macros in tracks/sql/macros)."""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def utc_timestamp(col: Column) -> Column:
    """ISO-8601 text with an offset ('Z' or '+HH:MM') -> UTC TIMESTAMP_NTZ."""
    local = F.to_timestamp_ntz(F.substring(col, 1, 19), F.lit("yyyy-MM-dd'T'HH:mm:ss"))
    sign = F.when(F.substring(col, 20, 1) == "-", F.lit(-1)).otherwise(F.lit(1))
    minutes = F.substring(col, 21, 2).cast("int") * 60 + F.substring(col, 24, 2).cast("int")
    offset = F.when(col.endswith("Z"), F.lit(0)).otherwise(sign * minutes)
    return local - F.make_dt_interval(F.lit(0), F.lit(0), offset)


def ist_date(ts: Column) -> Column:
    """The IST calendar date of a UTC timestamp (IST = UTC + 5:30)."""
    return F.to_date(ts + F.expr("INTERVAL 330 MINUTES"))


def to_paise(col: Column) -> Column:
    """Rupees as text ('₹12,345.50', '12,345.5', '499.00') -> integer paise, without floating point."""
    s = F.regexp_replace(F.trim(col), "[₹, ]", "")
    whole = F.substring_index(s, ".", 1).cast("bigint")
    frac = F.when(F.instr(s, ".") > 0, F.substring_index(s, ".", -1)).otherwise(F.lit(""))
    frac2 = F.rpad(F.when(frac == "", F.lit("0")).otherwise(frac), 2, "0").cast("bigint")
    return whole * 100 + frac2


def clean_key(col: Column) -> Column:
    """lower(trim(x)): how raw spellings are matched against the city map."""
    return F.lower(F.trim(col))
