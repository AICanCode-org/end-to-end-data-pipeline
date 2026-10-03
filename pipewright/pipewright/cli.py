"""The `pipewright` command line. Exit codes: 0 ok, 1 checks failed / not published, 2 usage, 3 a file has not
arrived (retry later), 4 source API error, 5 contract violation, 6 transformation failed."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import __version__, config, dq, explainer, ingest, runner, transform, warehouse
from .contract import ContractViolation
from .http import SourceError


def _days(args) -> list:
    if args.date:
        return [config.parse_date(args.date)]
    if args.start and args.end:
        return config.date_range(config.parse_date(args.start), config.parse_date(args.end))
    raise SystemExit("give --date, or --start and --end")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pipewright", description="BazaarBox's daily data pipeline.")
    ap.add_argument("--version", action="version", version=f"pipewright {__version__}")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name: str, help_: str, dates: bool = True) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_)
        if dates:
            p.add_argument("--date", help="business date YYYY-MM-DD")
            p.add_argument("--start")
            p.add_argument("--end")
        return p

    p = add("ingest", "load one source (or --all) for a date into the lake")
    p.add_argument("--source")
    p.add_argument("--all", action="store_true")
    add("check-drops", "exit 3 if a file drop for the date is missing")
    add("transform", "build silver + gold with the track in PIPEWRIGHT_TRACK")
    add("dq", "run the data-quality checks on the build")
    add("publish", "swap the checked build into place", dates=False)
    add("run", "ingest + transform + dq + publish for a date or a range")
    p = add("explain", "explain the latest data-quality report (AI or template)", dates=False)
    p.add_argument("--report", type=Path)
    p = add("export", "write the published gold tables as canonical CSV", dates=False)
    p.add_argument("--out", type=Path, required=True)
    args = ap.parse_args(argv)

    try:
        settings = config.load()
        if args.cmd == "ingest":
            if not (args.all or args.source):
                ap.error("give --source NAME or --all")
            for day in _days(args):
                results = ingest.ingest_all(settings, day) if args.all else [ingest.ingest(settings, args.source, day)]
                for r in results:
                    print(json.dumps(r.as_dict()))
        elif args.cmd == "check-drops":
            missing = {str(d): ingest.missing_drops(settings, d) for d in _days(args)}
            missing = {d: m for d, m in missing.items() if m}
            if missing:
                print(f"missing drops: {missing}")
                return 3
            print("all drops present")
        elif args.cmd == "transform":
            for day in _days(args):
                print(f"built {transform.run(settings, day)} ({settings.track}) for {day}")
        elif args.cmd == "dq":
            for day in _days(args):
                report = dq.run(settings, day)
                s = report["summary"]
                print(f"{day}: {s['passed']}/{s['checks']} passed, {s['failed_error']} blocking, {s['failed_warn']} warnings")
                for c in report["checks"]:
                    if c["status"] != "pass":
                        print(f"  {c['severity'].upper():5} {c['id']}: {c['detail']}")
                if not s["publishable"]:
                    print(f"not publishable: incident note {runner.write_incident(settings, day, report)}")
                    return 1
        elif args.cmd == "publish":
            print(f"published {warehouse.publish(settings)}")
        elif args.cmd == "run":
            runner.run_range(settings, _days(args))
        elif args.cmd == "explain":
            path = args.report or max(settings.reports_dir.glob("dq-*.json"), default=None)
            if path is None:
                print("no data-quality report yet: run `pipewright dq` first", file=sys.stderr)
                return 2
            print(explainer.render(explainer.explain(json.loads(Path(path).read_text()))))
        elif args.cmd == "export":
            from .export import export

            export(settings.warehouse, args.out, settings.contract)
            print(f"exported gold tables to {args.out}")
    except runner.RunFailed as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except warehouse.PublishBlocked as exc:
        print(f"not published: {exc}", file=sys.stderr)
        return 1
    except ingest.MissingDrop as exc:
        print(f"missing drop: {exc}", file=sys.stderr)
        return 3
    except SourceError as exc:
        print(f"source API error: {exc}", file=sys.stderr)
        return 4
    except ContractViolation as exc:
        print(f"contract violation: {exc}", file=sys.stderr)
        return 5
    except transform.TransformError as exc:
        print(str(exc), file=sys.stderr)
        return 6
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
