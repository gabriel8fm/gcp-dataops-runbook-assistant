"""Generate, load, validate and reprocess synthetic incident events.

Local mode uses a SQLite file that mirrors the BigQuery layers and needs no
credentials. GCP mode (--backend bigquery) loads the same batches into
BigQuery; transformation and checks then run in Dataform.
"""
import argparse
from contextlib import closing
from datetime import datetime, time, timedelta, timezone
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

from dataops import events, incident_query
from dataops import local_warehouse as warehouse

DEFAULT_LANDING = Path(".warehouse") / "landing"
DEMO_DIR = Path(".warehouse") / "demo"
# Fixed reference dates keep the demo output reproducible on any day.
DEMO_END = events.parse_date("2026-11-16")
DEMO_DAYS = 14
DATAFORM_CLI = "@dataform/cli@3.1.3"


def yesterday():
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def morning_after(day):
    return datetime.combine(day + timedelta(days=1), time(6), timezone.utc)


def resolve_range(args):
    end = events.parse_date(args.end) if args.end else yesterday()
    start = events.parse_date(args.start) if args.start else end - timedelta(days=DEMO_DAYS - 1)
    return events.date_range(start, end)


def generate(days, landing, seed, inject_faults=False):
    written = []
    for day in days:
        rows = events.generate_batch(day, seed)
        faults = None
        if inject_faults:
            rows, faults = events.inject_faults(rows)
        path = events.write_batch(landing, day, rows)
        written.append({
            "batch_date": day.isoformat(),
            "path": str(path),
            "rows": len(rows),
            "late_events": len(events.late_events(rows)),
            **({"injected_faults": faults} if faults else {}),
        })
    return written


def read_batch(landing, day):
    path = events.batch_path(landing, day)
    if not path.exists():
        raise SystemExit(f"Missing {path}. Run: python3 pipeline.py generate --start {day} --end {day}")
    return events.read_jsonl(path)


def bigquery_target(args):
    from dataops import bigquery_backend

    if not args.project:
        raise SystemExit("--project (or GOOGLE_CLOUD_PROJECT) is required for --backend bigquery")
    client = bigquery_backend.make_client(args.project, args.location)
    return bigquery_backend, client, bigquery_backend.raw_table_id(args.project)


def load(args, days, file_rows=None):
    results = []
    if args.backend == "local":
        with closing(warehouse.connect(args.db)) as connection:
            for day in days:
                rows = file_rows if file_rows is not None else read_batch(args.landing, day)
                results.append(warehouse.load_batch(connection, day, rows))
        return {"backend": "local", "db": str(args.db), "loads": results}
    backend, client, table_id = bigquery_target(args)
    with closing(client):
        for day in days:
            rows = file_rows if file_rows is not None else read_batch(args.landing, day)
            results.append(backend.load_batch(client, table_id, day, rows))
        counts = backend.partition_counts(client, table_id, days[0], days[-1])
    return {"backend": "bigquery", "table": table_id, "loads": results, "partitions": counts}


def dataform_command(args):
    return [
        "npx", "--yes", DATAFORM_CLI, "run", "dataform",
        f"--default-database={args.project}", f"--default-location={args.location}",
        "--job-labels=app=runbook-assistant,data=synthetic",
    ]


def transform(args):
    with closing(warehouse.connect(args.db)) as connection:
        return warehouse.transform(connection, args.as_of, args.freshness_hours)


def after_load(args, output):
    """Run the transformation step that matches the backend."""
    if args.backend == "local":
        output["transform"] = transform(args)
        return output
    command = dataform_command(args)
    if args.run_dataform:
        completed = subprocess.run(command, check=False)
        output["dataform_exit_code"] = completed.returncode
    else:
        output["next_step"] = "Run Dataform to validate and rebuild: " + shlex.join(command)
    return output


def demo(args):
    """Normal run, idempotent rerun, late event, quality failure, then reprocess."""
    root = Path(args.demo_dir)
    shutil.rmtree(root, ignore_errors=True)
    landing, db = root / "landing", root / "incidents.db"
    days = events.date_range(DEMO_END - timedelta(days=DEMO_DAYS - 1), DEMO_END)
    last = days[-1]
    generate(days, landing, args.seed)
    report, checks = {"db": str(db), "batches": [days[0].isoformat(), last.isoformat()]}, {}

    with closing(warehouse.connect(db)) as connection:
        def count(sql):
            return connection.execute(sql).fetchone()[0]

        for day in days[:-1]:
            warehouse.load_batch(connection, day, events.read_jsonl(events.batch_path(landing, day)))
        first = warehouse.transform(connection, morning_after(days[-2]), args.freshness_hours)
        raw_before = count("SELECT COUNT(*) FROM raw_incident_events")
        report["1_normal_run"] = {
            "status": first["status"], "raw_rows": raw_before,
            "validated_incidents": first["validated_rows"],
        }
        checks["normal_run_passes"] = first["status"] == "passed"

        warehouse.load_batch(connection, days[-2], events.read_jsonl(events.batch_path(landing, days[-2])))
        rerun = warehouse.transform(connection, morning_after(days[-2]), args.freshness_hours)
        raw_after = count("SELECT COUNT(*) FROM raw_incident_events")
        report["2_rerun_same_batch"] = {
            "batch_date": days[-2].isoformat(), "raw_rows_before": raw_before,
            "raw_rows_after": raw_after, "validated_incidents": rerun["validated_rows"],
        }
        checks["rerun_keeps_count"] = (
            raw_before == raw_after and rerun["validated_rows"] == first["validated_rows"]
        )

        last_rows = events.read_jsonl(events.batch_path(landing, last))
        known = {
            row["incident_id"]: row["event_count"]
            for row in connection.execute("SELECT incident_id, event_count FROM incidents_validated")
        }
        late_rows = events.late_events(last_rows)
        # Prefer a late event for an incident that is already validated.
        late = next((row for row in late_rows if row["incident_id"] in known), None)
        late = late or (late_rows[0] if late_rows else None)
        warehouse.load_batch(connection, last, last_rows)
        with_late = warehouse.transform(connection, morning_after(last), args.freshness_hours)
        report["3_late_event"] = {"status": with_late["status"], "event": late}
        # None means "not exercised": this seed delivered no late event on the last day.
        checks["late_event_incorporated"] = None
        if late:
            after = connection.execute(
                "SELECT status, event_count FROM incidents_validated WHERE incident_id = ?",
                (late["incident_id"],),
            ).fetchone()
            before = known.get(late["incident_id"], 0)
            report["3_late_event"]["incident_before"] = {"event_count": before}
            report["3_late_event"]["incident_after"] = dict(after) if after else None
            staged = connection.execute(
                "SELECT COUNT(*) FROM stg_incident_events WHERE event_id = ?", (late["event_id"],)
            ).fetchone()[0]
            checks["late_event_incorporated"] = (
                with_late["status"] == "passed"
                and staged == 1
                and after is not None
                and after["event_count"] > before
            )
        good_validated = count("SELECT COUNT(*) FROM incidents_validated")
        good_raw = count("SELECT COUNT(*) FROM raw_incident_events")

        faulty_rows, injected = events.inject_faults(last_rows)
        warehouse.load_batch(connection, last, faulty_rows)
        failed = warehouse.transform(connection, morning_after(last), args.freshness_hours)
        report["4_quality_failure"] = {
            "status": failed["status"],
            "failed_checks": failed["failed_checks"],
            "validated_table_updated": failed["validated_table_updated"],
            "validated_incidents": count("SELECT COUNT(*) FROM incidents_validated"),
            "diagnosis": {
                result["check"]: result["sample"] for result in failed["checks"] if not result["passed"]
            },
        }
        checks["quality_failure_detected"] = (
            failed["status"] == "failed"
            and set(injected) <= set(failed["failed_checks"])
            and count("SELECT COUNT(*) FROM incidents_validated") == good_validated
        )

        generate([last], landing, args.seed)
        warehouse.load_batch(connection, last, events.read_jsonl(events.batch_path(landing, last)))
        repaired = warehouse.transform(connection, morning_after(last), args.freshness_hours)
        report["5_reprocess"] = {
            "status": repaired["status"], "raw_rows": count("SELECT COUNT(*) FROM raw_incident_events"),
            "validated_incidents": repaired["validated_rows"],
        }
        checks["reprocess_restores_state"] = (
            repaired["status"] == "passed"
            and repaired["validated_rows"] == good_validated
            and count("SELECT COUNT(*) FROM raw_incident_events") == good_raw
        )

    import runbook

    with closing(runbook.build_index(runbook.load_data("runbooks.json"))) as index:
        runner = incident_query.LocalRunner(db)
        try:
            response = incident_query.answer(
                runbook.DEMO_QUESTION, runner, lambda text: runbook.retrieve(index, text),
                as_of=morning_after(last), freshness_hours=args.freshness_hours,
            )
        finally:
            runner.close()
    report["6_assistant"] = {
        "question": runbook.DEMO_QUESTION,
        "as_of": events.format_ts(morning_after(last)),
        "mode": response["mode"],
        "answer": response["answer"],
        "citations": response["citations"],
    }
    checks["assistant_cites_data_and_runbook"] = (
        response["mode"] == "data+extractive"
        and response["citations"][0].endswith(incident_query.QUERY_ID)
        and any(citation.startswith("RB-") for citation in response["citations"])
    )
    report["expectations"] = checks
    report["passed"] = all(value is not False for value in checks.values())
    return report


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    def add_range(command):
        command.add_argument("--start", help="First batch date (YYYY-MM-DD)")
        command.add_argument("--end", help="Last batch date (default: yesterday, UTC)")

    def add_common(command, backend=True):
        command.add_argument("--db", type=Path, default=warehouse.DEFAULT_DB, help="Local SQLite file")
        command.add_argument("--landing", type=Path, default=DEFAULT_LANDING)
        command.add_argument("--seed", type=int, default=events.DEFAULT_SEED)
        command.add_argument("--as-of", help="Reference time for checks (default: now, UTC)")
        command.add_argument(
            "--freshness-hours", type=int, default=warehouse.DEFAULT_FRESHNESS_HOURS
        )
        if backend:
            command.add_argument("--backend", choices=("local", "bigquery"), default="local")
            command.add_argument("--project", default=None, help="GCP project for --backend bigquery")
            command.add_argument("--location", default="US")
            command.add_argument(
                "--run-dataform", action="store_true",
                help="With --backend bigquery, run the Dataform CLI after loading",
            )

    gen = commands.add_parser("generate", help="Write deterministic batch files to the landing zone")
    add_range(gen)
    add_common(gen, backend=False)
    gen.add_argument("--inject-faults", action="store_true", help="Add one instance of each defect")

    load_cmd = commands.add_parser("load", help="Load landed batches (one partition per batch)")
    add_range(load_cmd)
    add_common(load_cmd)
    load_cmd.add_argument("--file", type=Path, help="Load this JSONL file instead of the landing zone")
    load_cmd.add_argument("--batch-date", help="Partition to replace when using --file")

    add_common(commands.add_parser("transform", help="Run checks and rebuild incidents_validated"), backend=False)
    add_common(commands.add_parser("check", help="Run quality checks only"), backend=False)

    run_cmd = commands.add_parser("run", help="Generate, load and transform a date range")
    add_range(run_cmd)
    add_common(run_cmd)

    reprocess = commands.add_parser(
        "reprocess", help="Regenerate and reload a date range, then validate again"
    )
    add_range(reprocess)
    add_common(reprocess)
    reprocess.add_argument(
        "--from-landing", action="store_true",
        help="Replay existing landing files instead of regenerating them",
    )

    add_common(commands.add_parser("status", help="Show partitions and the last quality run"), backend=False)

    demo_cmd = commands.add_parser("demo", help="Run the full offline scenario and verify it")
    demo_cmd.add_argument("--demo-dir", type=Path, default=DEMO_DIR)
    demo_cmd.add_argument("--seed", type=int, default=events.DEFAULT_SEED)
    demo_cmd.add_argument(
        "--freshness-hours", type=int, default=warehouse.DEFAULT_FRESHNESS_HOURS
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "backend", "local") == "bigquery" and not args.project:
        import os

        args.project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    exit_code = 0
    if args.command == "generate":
        output = {"written": generate(resolve_range(args), args.landing, args.seed, args.inject_faults)}
    elif args.command == "load":
        if args.file:
            if not args.batch_date:
                parser.error("--file requires --batch-date")
            output = load(args, [events.parse_date(args.batch_date)], events.read_jsonl(args.file))
        else:
            output = load(args, resolve_range(args))
        if args.run_dataform:
            if args.backend != "bigquery":
                parser.error("--run-dataform requires --backend bigquery")
            output = after_load(args, output)
            exit_code = output["dataform_exit_code"]
    elif args.command in ("transform", "check"):
        if args.command == "transform":
            output = transform(args)
        else:
            with closing(warehouse.connect(args.db)) as connection:
                results = warehouse.run_checks(connection, args.as_of, args.freshness_hours)
            output = {
                "status": "passed" if all(item["passed"] for item in results) else "failed",
                "checks": results,
            }
        exit_code = int(output["status"] != "passed")
    elif args.command in ("run", "reprocess"):
        days = resolve_range(args)
        output = {"range": [days[0].isoformat(), days[-1].isoformat()]}
        if not getattr(args, "from_landing", False):
            output["generated"] = len(generate(days, args.landing, args.seed))
        output.update(load(args, days))
        output = after_load(args, output)
        if "transform" in output:
            exit_code = int(output["transform"]["status"] != "passed")
        elif "dataform_exit_code" in output:
            exit_code = output["dataform_exit_code"]
    elif args.command == "status":
        with closing(warehouse.connect(args.db)) as connection:
            output = warehouse.status(connection)
    else:
        output = demo(args)
        exit_code = int(not output["passed"])
    print(json.dumps(output, indent=2, ensure_ascii=False, default=str))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
