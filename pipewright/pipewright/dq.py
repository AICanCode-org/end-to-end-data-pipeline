"""Data-quality checks on a build before it is published (the "Great Expectations" layer, shared by both tracks).

Checks are declared in contract/data-contract.yaml (`checks:`). Each one runs one SQL query against the gold
schema and yields a result with an observed value and a status. Only aggregates are recorded: never a row, so
the report (and the AI explainer that reads it) cannot leak customer data. On the SQL track the dbt tests'
results (target/run_results.json) are merged into the same report.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

from . import contract as contract_mod
from .config import Settings
from .transform import command

SAFE_VALUE = re.compile(r"^[A-Za-z0-9_.\- ]{1,40}$")


@dataclass
class CheckResult:
    id: str
    kind: str
    table: str
    severity: str
    status: str  # pass | fail | error (the check itself could not run)
    observed: float | int | None
    threshold: float | int | None
    detail: str
    unexpected_values: list[str] = field(default_factory=list)

    @property
    def blocking(self) -> bool:
        return self.severity == "error" and self.status != "pass"


def _enum_sql(values: list[str]) -> str:
    return "(" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + ")"


def _resolve(text: str, enums: dict[str, list[str]]) -> str:
    return re.sub(r"@(\w+)", lambda m: _enum_sql(enums[m.group(1)]), text)


def run_check(con: duckdb.DuckDBPyConnection, check: dict, enums: dict[str, list[str]]) -> CheckResult:
    kind, table, sev = check["kind"], check.get("table", "-"), check.get("severity", "error")
    limit = check.get("max_failing", 0)

    def result(failing: bool, observed, threshold, detail: str, values: list[str] | None = None) -> CheckResult:
        return CheckResult(check["id"], kind, table, sev, "fail" if failing else "pass", observed, threshold, detail, values or [])

    try:
        if kind in ("row_count_min", "row_count_max"):
            n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            bound = check["min"] if kind == "row_count_min" else check["max"]
            bad = n < bound if kind == "row_count_min" else n > bound
            return result(bad, n, bound, f"{table} has {n} rows ({'min' if kind == 'row_count_min' else 'max'} {bound})")
        if kind == "accepted_values":
            col, allowed = check["column"], _resolve(check["values"], enums) if isinstance(check["values"], str) else _enum_sql(check["values"])
            where = f"{col} IS NULL OR {col} NOT IN {allowed}"
            n = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
            vals = [str(v[0]) for v in con.execute(f"SELECT DISTINCT {col} FROM {table} WHERE {where} ORDER BY 1 NULLS FIRST LIMIT 5").fetchall()]
            vals = [v if SAFE_VALUE.match(v) else "<unprintable>" for v in vals]
            return result(n > limit, n, limit, f"{n} row(s) of {table}.{col} outside the accepted values", vals)
        if kind == "relationship":
            col, (to_table, to_col) = check["column"], check["to"].split(".")
            n = con.execute(f"SELECT count(*) FROM {table} WHERE {col} IS NOT NULL AND {col} NOT IN (SELECT {to_col} FROM {to_table})").fetchone()[0]
            return result(n > limit, n, limit, f"{n} row(s) of {table}.{col} have no match in {check['to']}")
        if kind == "max_share":
            total, hits = con.execute(f"SELECT count(*), count(*) FILTER (WHERE {check['where']}) FROM {table}").fetchone()
            share = round(hits / total, 6) if total else 0.0
            return result(share > check["max"], share, check["max"], f"{hits} of {total} rows of {table} match {check['where']}")
        if kind == "expression":
            n = con.execute(f"SELECT count(*) FROM {table} WHERE NOT ({check['expect']})").fetchone()[0]
            return result(n > limit, n, limit, f"{n} row(s) of {table} break: {check['expect']}")
        if kind == "reconcile":
            left = con.execute(_resolve(check["left"], enums)).fetchone()[0] or 0
            right = con.execute(_resolve(check["right"], enums)).fetchone()[0] or 0
            diff = int(left) - int(right)
            return result(diff != 0, diff, 0, f"left - right = {diff}")
        raise ValueError(f"unknown check kind {kind!r}")
    except (duckdb.Error, KeyError, ValueError) as exc:
        return CheckResult(check["id"], kind, table, sev, "error", None, None, f"check could not run: {type(exc).__name__}")


def dbt_test_results(settings: Settings, day: dt.date) -> list[CheckResult]:
    """Run the dbt data tests (not the unit tests) on the build and turn run_results.json into CheckResults."""
    cmd, env, cwd = command(settings, day)
    cmd = [cmd[0], "test", "--exclude", "test_type:unit", *cmd[cmd.index("--project-dir"):]]
    subprocess.run(cmd, env=env, cwd=cwd, capture_output=True, text=True, check=False)  # noqa: S603
    path = settings.warehouse.parent / "dbt-target" / "run_results.json"
    if not path.exists():
        return [CheckResult("dbt_tests", "dbt", "-", "error", "error", None, None, "dbt test produced no run_results.json")]
    out = []
    for r in json.loads(path.read_text())["results"]:
        name = r["unique_id"].split(".")[2]
        failures = r.get("failures") or 0
        sev = "warn" if r["status"] == "warn" else "error"
        status = {"pass": "pass", "warn": "fail", "fail": "fail"}.get(r["status"], "error")
        out.append(CheckResult(f"dbt:{name}"[:80], "dbt", "-", sev, status, failures, 0, f"dbt test {r['status']} ({failures} failing rows)"))
    return out


def run(settings: Settings, day: dt.date, db: Path | None = None) -> dict:
    db = db or settings.build_warehouse
    c = contract_mod.load(settings.contract)
    con = duckdb.connect(str(db))
    try:
        con.execute("SET search_path = 'gold'")
        results = [run_check(con, check, c["enums"]) for check in c["checks"]]
    finally:
        con.close()
    if settings.track == "sql":
        results += dbt_test_results(settings, day)
    report = make_report(settings, day, c["version"], results)
    save(settings, db, report)
    return report


def make_report(settings: Settings, day: dt.date, version: str, results: list[CheckResult]) -> dict:
    blocking = [r for r in results if r.blocking]
    return {
        "run_date": day.isoformat(),
        "track": settings.track,
        "contract_version": version,
        "summary": {
            "checks": len(results),
            "passed": sum(r.status == "pass" for r in results),
            "failed_error": len(blocking),
            "failed_warn": sum(r.severity == "warn" and r.status != "pass" for r in results),
            "publishable": not blocking,
        },
        "checks": [asdict(r) for r in results],
    }


def save(settings: Settings, db: Path, report: dict) -> Path:
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    path = settings.reports_dir / f"dq-{report['run_date']}.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    con = duckdb.connect(str(db))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.execute(
            "CREATE OR REPLACE TABLE ops.dq_results (run_date DATE, check_id VARCHAR, kind VARCHAR, table_name VARCHAR, "
            "severity VARCHAR, status VARCHAR, observed DOUBLE, threshold DOUBLE, detail VARCHAR)"
        )
        con.executemany(
            "INSERT INTO ops.dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(report["run_date"], r["id"], r["kind"], r["table"], r["severity"], r["status"], r["observed"], r["threshold"], r["detail"])
             for r in report["checks"]],
        )
        con.execute("CREATE OR REPLACE TABLE ops.build_info AS SELECT ?::DATE AS run_date, ? AS track, ? AS contract_version, ? AS publishable",
                    [report["run_date"], report["track"], report["contract_version"], report["summary"]["publishable"]])
    finally:
        con.close()
    return path
