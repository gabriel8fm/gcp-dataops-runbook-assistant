"""Local SQLite stand-in for the BigQuery raw, staging and validated layers.

The SQL mirrors the Dataform project in dataform/definitions so the same
contracts can be exercised offline:

* load_batch replaces one batch_date partition in a transaction, matching a
  BigQuery load job that writes to ``incident_events$YYYYMMDD`` with
  WRITE_TRUNCATE.
* run_checks executes the same assertions as the Dataform assertion files. Each
  query returns offending rows; zero rows means the check passed.
* transform rebuilds incidents_validated only when every check passes, so a bad
  batch never replaces the last good validated snapshot.
"""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

from dataops import events

DEFAULT_DB = Path(".warehouse") / "incidents.db"
DEFAULT_FRESHNESS_HOURS = 48
CLOCK_SKEW_MINUTES = 5

DDL = """
CREATE TABLE IF NOT EXISTS raw_incident_events (
    event_id TEXT,
    incident_id TEXT,
    sequence INTEGER,
    service TEXT,
    severity TEXT,
    status TEXT,
    summary TEXT,
    event_ts TEXT,
    batch_date TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS raw_incident_events_batch_date
    ON raw_incident_events (batch_date);

-- Exact redeliveries collapse to one row; conflicting payloads stay visible.
CREATE VIEW IF NOT EXISTS stg_incident_events AS
SELECT
    event_id, incident_id, sequence, service, severity, status, summary, event_ts,
    MIN(batch_date) AS first_batch_date,
    MAX(batch_date) AS last_batch_date,
    COUNT(*) AS delivery_count
FROM raw_incident_events
WHERE event_id IS NOT NULL
GROUP BY event_id, incident_id, sequence, service, severity, status, summary, event_ts;

CREATE TABLE IF NOT EXISTS incidents_validated (
    incident_id TEXT PRIMARY KEY,
    service TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    summary TEXT,
    opened_at TEXT NOT NULL,
    acknowledged_at TEXT,
    mitigated_at TEXT,
    resolved_at TEXT,
    last_event_ts TEXT NOT NULL,
    is_open INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    last_batch_date TEXT NOT NULL,
    _validated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quality_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    as_of TEXT NOT NULL,
    status TEXT NOT NULL,
    failed_checks TEXT NOT NULL,
    results TEXT NOT NULL,
    validated_rows INTEGER
);
"""

SEVERITY_LIST = ", ".join(f"'{value}'" for value in events.SEVERITIES)
STATUS_LIST = ", ".join(f"'{value}'" for value in events.STATUSES)

# Names match dataform/definitions/assertions/<name>.sqlx.
CHECKS = {
    "assert_event_id_not_null": (
        "keys",
        """SELECT batch_date, incident_id, sequence FROM raw_incident_events
           WHERE event_id IS NULL ORDER BY batch_date, incident_id, sequence""",
    ),
    "assert_event_id_unique": (
        "keys",
        """SELECT event_id, COUNT(*) AS payload_versions FROM stg_incident_events
           GROUP BY event_id HAVING COUNT(*) > 1 ORDER BY event_id""",
    ),
    "assert_required_fields_not_null": (
        "required fields",
        """SELECT event_id, batch_date FROM raw_incident_events
           WHERE incident_id IS NULL OR sequence IS NULL OR service IS NULL
              OR severity IS NULL OR status IS NULL OR event_ts IS NULL
           ORDER BY batch_date, event_id""",
    ),
    "assert_valid_severity": (
        "accepted values",
        f"""SELECT event_id, batch_date, severity FROM raw_incident_events
            WHERE severity IS NOT NULL AND severity NOT IN ({SEVERITY_LIST})
            ORDER BY batch_date, event_id""",
    ),
    "assert_valid_status": (
        "accepted values",
        f"""SELECT event_id, batch_date, status FROM raw_incident_events
            WHERE status IS NOT NULL AND status NOT IN ({STATUS_LIST})
            ORDER BY batch_date, event_id""",
    ),
    "assert_event_ts_not_future": (
        "timestamps",
        """SELECT event_id, batch_date, event_ts FROM raw_incident_events
           WHERE event_ts > :future_threshold ORDER BY batch_date, event_id""",
    ),
    "assert_incident_single_service": (
        "keys",
        """SELECT incident_id, COUNT(DISTINCT service) AS services FROM raw_incident_events
           WHERE incident_id IS NOT NULL
           GROUP BY incident_id HAVING COUNT(DISTINCT service) > 1 ORDER BY incident_id""",
    ),
    "assert_freshness": (
        "freshness",
        """SELECT latest_event_ts FROM (
               SELECT MAX(event_ts) AS latest_event_ts FROM raw_incident_events
               WHERE event_ts <= :future_threshold
           ) WHERE latest_event_ts IS NULL OR latest_event_ts < :freshness_threshold""",
    ),
}

BUILD_VALIDATED = """
INSERT INTO incidents_validated
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY incident_id ORDER BY sequence DESC, event_ts DESC, event_id DESC
    ) AS position
    FROM stg_incident_events
),
lifecycle AS (
    SELECT
        incident_id,
        MIN(event_ts) AS opened_at,
        MIN(CASE WHEN status = 'acknowledged' THEN event_ts END) AS acknowledged_at,
        MIN(CASE WHEN status = 'mitigated' THEN event_ts END) AS mitigated_at,
        MIN(CASE WHEN status = 'resolved' THEN event_ts END) AS resolved_at,
        MAX(event_ts) AS last_event_ts,
        COUNT(*) AS event_count,
        MAX(last_batch_date) AS last_batch_date
    FROM stg_incident_events
    GROUP BY incident_id
)
SELECT
    ranked.incident_id, ranked.service, ranked.severity, ranked.status, ranked.summary,
    lifecycle.opened_at, lifecycle.acknowledged_at, lifecycle.mitigated_at,
    lifecycle.resolved_at, lifecycle.last_event_ts,
    ranked.status <> 'resolved' AS is_open,
    lifecycle.event_count, lifecycle.last_batch_date, :validated_at
FROM ranked JOIN lifecycle USING (incident_id)
WHERE ranked.position = 1
"""


def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)


def to_utc(value):
    if value is None:
        return utc_now()
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def connect(db_path=DEFAULT_DB):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(DDL)
    return connection


def _normalize(row, batch_date, ingested_at):
    event_ts = row.get("event_ts")
    if event_ts is not None:
        # BigQuery rejects a load with an unparseable TIMESTAMP; mirror that.
        event_ts = events.format_ts(to_utc(event_ts))
    return (
        row.get("event_id"), row.get("incident_id"), row.get("sequence"),
        row.get("service"), row.get("severity"), row.get("status"),
        row.get("summary"), event_ts, batch_date, ingested_at,
    )


def load_batch(connection, batch_date, rows, ingested_at=None):
    """Atomically replace one batch_date partition. Safe to repeat."""
    batch_date = events.parse_date(batch_date).isoformat()
    ingested_at = events.format_ts(to_utc(ingested_at))
    values = [_normalize(row, batch_date, ingested_at) for row in rows]
    with connection:
        replaced = connection.execute(
            "DELETE FROM raw_incident_events WHERE batch_date = ?", (batch_date,)
        ).rowcount
        connection.executemany(
            "INSERT INTO raw_incident_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", values
        )
    return {"batch_date": batch_date, "rows_loaded": len(values), "rows_replaced": replaced}


def check_parameters(as_of, freshness_hours=DEFAULT_FRESHNESS_HOURS):
    as_of = to_utc(as_of)
    return {
        "future_threshold": events.format_ts(as_of + timedelta(minutes=CLOCK_SKEW_MINUTES)),
        "freshness_threshold": events.format_ts(as_of - timedelta(hours=freshness_hours)),
    }


def run_checks(connection, as_of=None, freshness_hours=DEFAULT_FRESHNESS_HOURS, sample_size=5):
    parameters = check_parameters(as_of, freshness_hours)
    results = []
    for name, (category, sql) in CHECKS.items():
        failing = [dict(row) for row in connection.execute(sql, parameters)]
        results.append({
            "check": name,
            "category": category,
            "passed": not failing,
            "failing_rows": len(failing),
            "sample": failing[:sample_size],
        })
    return results


def transform(connection, as_of=None, freshness_hours=DEFAULT_FRESHNESS_HOURS):
    """Run checks, then rebuild incidents_validated only if all of them pass."""
    as_of = to_utc(as_of)
    results = run_checks(connection, as_of, freshness_hours)
    failed = [result["check"] for result in results if not result["passed"]]
    validated_rows = None
    with connection:
        if not failed:
            connection.execute("DELETE FROM incidents_validated")
            connection.execute(BUILD_VALIDATED, {"validated_at": events.format_ts(as_of)})
            validated_rows = connection.execute(
                "SELECT COUNT(*) FROM incidents_validated"
            ).fetchone()[0]
        connection.execute(
            """INSERT INTO quality_runs (run_at, as_of, status, failed_checks, results, validated_rows)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                events.format_ts(utc_now()), events.format_ts(as_of),
                "failed" if failed else "passed", json.dumps(failed), json.dumps(results),
                validated_rows,
            ),
        )
    return {
        "status": "failed" if failed else "passed",
        "as_of": events.format_ts(as_of),
        "failed_checks": failed,
        "validated_rows": validated_rows,
        "validated_table_updated": not failed,
        "checks": results,
    }


def status(connection):
    batches = connection.execute(
        """SELECT batch_date, COUNT(*) AS rows, MAX(ingested_at) AS ingested_at
           FROM raw_incident_events GROUP BY batch_date ORDER BY batch_date"""
    ).fetchall()
    last_run = connection.execute(
        """SELECT run_at, as_of, status, failed_checks, validated_rows
           FROM quality_runs ORDER BY run_id DESC LIMIT 1"""
    ).fetchone()
    return {
        "raw_rows": connection.execute("SELECT COUNT(*) FROM raw_incident_events").fetchone()[0],
        "distinct_event_ids": connection.execute(
            "SELECT COUNT(DISTINCT event_id) FROM raw_incident_events"
        ).fetchone()[0],
        "validated_incidents": connection.execute(
            "SELECT COUNT(*) FROM incidents_validated"
        ).fetchone()[0],
        "batches": [dict(row) for row in batches],
        "last_quality_run": None if last_run is None else {
            **dict(last_run), "failed_checks": json.loads(last_run["failed_checks"]),
        },
    }
