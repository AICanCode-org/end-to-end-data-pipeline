"""1.1 monitoring: run history, data-quality history, freshness / volume / SLA checks and alerts.

Everything lives in <warehouse dir>/ops.duckdb (tables run_log, dq_history, monitor_results, alerts) and in
reports/alerts.jsonl (every alert). ops.duckdb is rewritten copy-and-replace, like the warehouse, so the dashboard can always
open it read-only. Alerts of ALERT_MIN_SEVERITY (default error) or worse are also POSTed to ALERT_WEBHOOK_URL (a Slack-style incoming webhook); delivery
problems are logged without the URL (it often contains a secret) and never fail the pipeline.

Checks are evaluated "as of" a moment (`now`, default the wall clock) so a drill or a test can replay a morning:
before a deadline a missing partition is only "pending"; after it, it is a failure.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import statistics
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from . import contract as contract_mod
from .config import IST, Settings

SCHEMA = {
    "run_log": "run_date DATE, track VARCHAR, kind VARCHAR, status VARCHAR, incremental BOOLEAN, since DATE, "
               "started_at TIMESTAMP, finished_at TIMESTAMP, detail VARCHAR",
    "dq_history": "run_date DATE, track VARCHAR, contract_version VARCHAR, check_id VARCHAR, severity VARCHAR, "
                  "status VARCHAR, observed DOUBLE, recorded_at TIMESTAMP",
    "monitor_results": "run_date DATE, checked_at TIMESTAMP, check_id VARCHAR, kind VARCHAR, severity VARCHAR, "
                       "status VARCHAR, detail VARCHAR",
    "alerts": "run_date DATE, raised_at TIMESTAMP, check_id VARCHAR, severity VARCHAR, message VARCHAR",
}


@dataclass
class MonitorResult:
    check_id: str
    kind: str  # freshness | volume | sla | dq
    severity: str
    status: str  # pass | pending | fail
    detail: str


def ops_path(settings: Settings) -> Path:
    return settings.warehouse.parent / "ops.duckdb"


def _utc_naive(moment: dt.datetime) -> dt.datetime:
    return moment.astimezone(dt.UTC).replace(tzinfo=None)


def write_ops(settings: Settings, fn: Callable[[duckdb.DuckDBPyConnection], None]) -> Path:
    """Apply `fn` to a copy of ops.duckdb, then swap it into place atomically."""
    path = ops_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".ops-{os.getpid()}.duckdb")
    tmp.unlink(missing_ok=True)
    if path.exists():
        shutil.copyfile(path, tmp)
    con = duckdb.connect(str(tmp))
    try:
        for table, cols in SCHEMA.items():
            con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")
        fn(con)
    finally:
        con.close()
    os.replace(tmp, path)
    return path


def read_ops(settings: Settings, sql: str, params: list | None = None) -> list[tuple]:
    path = ops_path(settings)
    if not path.exists():
        return []
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def record_run(settings: Settings, day: dt.date, kind: str, status: str, started: dt.datetime, detail: str = "",
               incremental: bool | None = None, since: dt.date | None = None) -> None:
    row = (day, settings.track, kind, status, incremental, since, _utc_naive(started), _utc_naive(dt.datetime.now(dt.UTC)), detail[:500])
    write_ops(settings, lambda con: con.execute("INSERT INTO run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(row)))


def record_dq(settings: Settings, report: dict) -> None:
    """Append a data-quality report to the history (aggregates only, like the report itself)."""
    now = _utc_naive(dt.datetime.now(dt.UTC))
    rows = [(report["run_date"], report["track"], report["contract_version"], c["id"], c["severity"], c["status"],
             c["observed"], now) for c in report["checks"]]
    if rows:
        write_ops(settings, lambda con: con.executemany("INSERT INTO dq_history VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows))


def _deadline(day: dt.date, hhmm: str) -> dt.datetime:
    h, m = (int(x) for x in hhmm.split(":"))
    return dt.datetime.combine(day + dt.timedelta(days=1), dt.time(h, m), IST)


def _manifest(settings: Settings, source: str, day: dt.date) -> dict | None:
    path = settings.lake_dir / "_manifests" / source / f"{day.isoformat()}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _arrived(settings: Settings, source: str, spec: dict, day: dt.date) -> tuple[bool, str]:
    """File sources: has the drop arrived (or already been loaded)? API sources: is the day in the lake?"""
    if _manifest(settings, source, day) is not None:
        return True, "is in the lake"
    if spec.get("kind") == "file" and (settings.drops_dir / spec["pattern"].format(date=day.isoformat())).is_file():
        return True, "drop file has arrived"
    return False, "drop file has not arrived" if spec.get("kind") == "file" else "is not in the lake"


def check_freshness(settings: Settings, day: dt.date, now: dt.datetime, cfg: dict) -> list[MonitorResult]:
    sources = contract_mod.load(settings.contract)["sources"]
    out = []
    for source, rule in cfg.items():
        due = _deadline(day, rule["expected_by"])
        sev = rule.get("severity", "error")
        cid = f"freshness:{source}"
        ok, what = _arrived(settings, source, sources.get(source, {}), day)
        if ok:
            out.append(MonitorResult(cid, "freshness", sev, "pass", f"{source} for {day} {what}"))
        elif now < due:
            out.append(MonitorResult(cid, "freshness", sev, "pending", f"{source} for {day} {what} yet (due {rule['expected_by']} IST)"))
        else:
            out.append(MonitorResult(cid, "freshness", sev, "fail", f"{source} for {day} {what} by {rule['expected_by']} IST"))
    return out


def check_volume(settings: Settings, day: dt.date, cfg: dict) -> list[MonitorResult]:
    out = []
    for source, rule in cfg.items():
        sev, cid = rule.get("severity", "warn"), f"volume:{source}"
        today = _manifest(settings, source, day)
        history = [m["rows"] for i in range(1, 8) if (m := _manifest(settings, source, day - dt.timedelta(days=i)))]
        if today is None or len(history) < 3:
            why = f"no {source} partition for {day} yet" if today is None else f"not enough history for {source} ({len(history)} day(s))"
            out.append(MonitorResult(cid, "volume", sev, "pending", why))
            continue
        median = statistics.median(history)
        ratio = round(today["rows"] / median, 3) if median else 0.0
        ok = rule["min_ratio"] <= ratio <= rule["max_ratio"]
        out.append(MonitorResult(cid, "volume", sev, "pass" if ok else "fail",
                                 f"{source}: {today['rows']} rows vs 7-day median {median:g} (ratio {ratio}, allowed {rule['min_ratio']}-{rule['max_ratio']})"))
    return out


def published_date(settings: Settings) -> dt.date | None:
    if not settings.warehouse.exists():
        return None
    con = duckdb.connect(str(settings.warehouse), read_only=True)
    try:
        return con.execute("SELECT run_date FROM ops.build_info").fetchone()[0]
    except (duckdb.Error, TypeError):
        return None
    finally:
        con.close()


def check_sla(settings: Settings, day: dt.date, now: dt.datetime, cfg: dict) -> MonitorResult:
    due, sev = _deadline(day, cfg["publish_by"]), cfg.get("severity", "error")
    published = published_date(settings)
    if published is not None and published >= day:
        return MonitorResult("sla:publish", "sla", sev, "pass", f"data for {day} is published")
    if now < due:
        return MonitorResult("sla:publish", "sla", sev, "pending", f"data for {day} not published yet (due {cfg['publish_by']} IST)")
    return MonitorResult("sla:publish", "sla", sev, "fail",
                         f"data for {day} not published by {cfg['publish_by']} IST (dashboard shows {published or 'nothing'})")


def check_dq(settings: Settings, day: dt.date) -> MonitorResult:
    path = settings.reports_dir / f"dq-{day.isoformat()}.json"
    if not path.exists():
        return MonitorResult("dq:gate", "dq", "error", "pending", f"no data-quality report for {day} yet")
    s = json.loads(path.read_text(encoding="utf-8"))["summary"]
    if s["failed_error"]:
        return MonitorResult("dq:gate", "dq", "error", "fail", f"{s['failed_error']} blocking check(s) failed for {day}: build not published")
    status = "pass" if not s["failed_warn"] else "fail"
    return MonitorResult("dq:gate" if status == "pass" else "dq:warnings", "dq", "error" if status == "pass" else "warn", status,
                         f"{s['passed']}/{s['checks']} checks passed, {s['failed_warn']} warning(s)")


def evaluate(settings: Settings, day: dt.date, now: dt.datetime | None = None) -> list[MonitorResult]:
    now = now or dt.datetime.now(dt.UTC)
    cfg = contract_mod.load(settings.contract).get("monitoring", {})
    return [
        *check_freshness(settings, day, now, cfg.get("freshness", {})),
        *check_volume(settings, day, cfg.get("volume", {})),
        *([check_sla(settings, day, now, cfg["sla"])] if "sla" in cfg else []),
        check_dq(settings, day),
    ]


def send_webhook(url: str, alerts: list[dict], timeout: float = 5.0) -> str:
    """POST the alerts as one JSON message. Returns a status line that never contains the URL."""
    if not url.lower().startswith(("https://", "http://")):
        return "webhook skipped: ALERT_WEBHOOK_URL must be http(s)"
    text = "\n".join(f"[{a['severity'].upper()}] {a['run_date']} {a['check_id']}: {a['message']}" for a in alerts)
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(), headers={"Content-Type": "application/json"}, method="POST")  # noqa: S310  # scheme checked above
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310  # scheme checked above
            return f"webhook delivered (HTTP {resp.status})"
    except urllib.error.HTTPError as exc:
        return f"webhook delivery failed (HTTP {exc.code})"
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return f"webhook delivery failed ({type(exc).__name__})"


def run(settings: Settings, day: dt.date, now: dt.datetime | None = None, env: dict[str, str] | None = None,
        log: Callable[[str], None] = print) -> tuple[list[MonitorResult], list[dict]]:
    env = dict(os.environ if env is None else env)
    now = now or dt.datetime.now(dt.UTC)
    results = evaluate(settings, day, now)
    alerts = [{"run_date": day.isoformat(), "raised_at": now.astimezone(IST).isoformat(timespec="minutes"), "check_id": r.check_id,
               "severity": r.severity, "message": r.detail} for r in results if r.status == "fail"]
    checked = _utc_naive(now)

    def save(con: duckdb.DuckDBPyConnection) -> None:
        con.execute("DELETE FROM monitor_results WHERE run_date = ?", [day])
        rows = [(day, checked, r.check_id, r.kind, r.severity, r.status, r.detail) for r in results]
        if rows:
            con.executemany("INSERT INTO monitor_results VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
        if alerts:
            con.executemany("INSERT INTO alerts VALUES (?, ?, ?, ?, ?)", [(day, checked, a["check_id"], a["severity"], a["message"]) for a in alerts])

    write_ops(settings, save)
    if alerts:
        settings.reports_dir.mkdir(parents=True, exist_ok=True)
        with (settings.reports_dir / "alerts.jsonl").open("a", encoding="utf-8") as f:
            for a in alerts:
                f.write(json.dumps(a) + "\n")
        floor = 0 if env.get("ALERT_MIN_SEVERITY", "error").lower() == "warn" else 1
        loud = [a for a in alerts if {"warn": 0, "error": 1}.get(a["severity"], 1) >= floor]
        if env.get("ALERT_WEBHOOK_URL") and loud:
            log(send_webhook(env["ALERT_WEBHOOK_URL"], loud))
    return results, alerts


def as_dicts(results: list[MonitorResult]) -> list[dict]:
    return [asdict(r) for r in results]
