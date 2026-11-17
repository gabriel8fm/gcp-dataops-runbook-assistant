"""Offline pipeline tests against the SQLite stand-in for BigQuery."""
from contextlib import closing, redirect_stdout
from datetime import timedelta
import io
import json
from pathlib import Path
import re
import tempfile
import unittest

from dataops import events
from dataops import local_warehouse as warehouse
import pipeline

ROOT = Path(__file__).resolve().parent.parent
DAYS = events.date_range("2026-11-03", "2026-11-16")
AS_OF = "2026-11-17T06:00:00Z"


def count(connection, sql, *parameters):
    return connection.execute(sql, parameters).fetchone()[0]


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.db = self.enterContext(closing(warehouse.connect(self.tmp / "test.db")))

    def load(self, days, rows_for=events.generate_batch):
        for day in days:
            warehouse.load_batch(self.db, day, rows_for(day), ingested_at=AS_OF)

    def test_same_batch_twice_keeps_counts(self):
        self.load(DAYS)
        first = warehouse.transform(self.db, AS_OF)
        raw = count(self.db, "SELECT COUNT(*) FROM raw_incident_events")

        result = warehouse.load_batch(self.db, DAYS[-1], events.generate_batch(DAYS[-1]))
        second = warehouse.transform(self.db, AS_OF)

        self.assertEqual(result["rows_replaced"], result["rows_loaded"])
        self.assertEqual(count(self.db, "SELECT COUNT(*) FROM raw_incident_events"), raw)
        self.assertEqual(second["validated_rows"], first["validated_rows"])
        self.assertEqual(
            count(self.db, "SELECT COUNT(DISTINCT event_id) FROM raw_incident_events"), raw
        )

    def test_late_event_is_incorporated(self):
        self.load(DAYS[:-1])
        warehouse.transform(self.db, "2026-11-16T06:00:00Z")
        late = next(
            row for row in events.late_events(events.generate_batch(DAYS[-1]))
            if count(self.db, "SELECT COUNT(*) FROM incidents_validated WHERE incident_id = ?",
                     row["incident_id"])
        )
        before = self.db.execute(
            "SELECT status, event_count FROM incidents_validated WHERE incident_id = ?",
            (late["incident_id"],),
        ).fetchone()
        self.assertLess(late["event_ts"][:10], DAYS[-1].isoformat())

        self.load(DAYS[-1:])
        result = warehouse.transform(self.db, AS_OF)

        after = self.db.execute(
            "SELECT status, event_count, last_batch_date FROM incidents_validated WHERE incident_id = ?",
            (late["incident_id"],),
        ).fetchone()
        self.assertEqual(result["status"], "passed")
        self.assertGreater(after["event_count"], before["event_count"])
        self.assertEqual(after["last_batch_date"], DAYS[-1].isoformat())
        self.assertEqual(
            count(self.db, "SELECT COUNT(*) FROM stg_incident_events WHERE event_id = ?",
                  late["event_id"]),
            1,
        )

    def test_redelivered_event_is_deduplicated(self):
        self.load(DAYS)
        warehouse.transform(self.db, AS_OF)
        validated = count(self.db, "SELECT COUNT(*) FROM incidents_validated")
        redelivered = events.generate_batch(DAYS[-2])[:3]
        # Same events arrive again in the next partition (at-least-once delivery).
        warehouse.load_batch(self.db, DAYS[-1], events.generate_batch(DAYS[-1]) + redelivered)
        result = warehouse.transform(self.db, AS_OF)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["validated_rows"], validated)

    def test_faulty_batch_triggers_checks_and_keeps_last_good_snapshot(self):
        self.load(DAYS)
        good = warehouse.transform(self.db, AS_OF)
        snapshot = self.db.execute("SELECT * FROM incidents_validated ORDER BY incident_id").fetchall()
        sample = events.read_jsonl(ROOT / "data" / "samples" / "faulty_batch_2026-11-16.jsonl")

        warehouse.load_batch(self.db, DAYS[-1], sample)
        failed = warehouse.transform(self.db, AS_OF)

        self.assertEqual(good["status"], "passed")
        self.assertEqual(failed["status"], "failed")
        self.assertFalse(failed["validated_table_updated"])
        self.assertEqual(set(failed["failed_checks"]), {
            "assert_event_id_not_null", "assert_event_id_unique",
            "assert_required_fields_not_null", "assert_valid_severity",
            "assert_event_ts_not_future", "assert_incident_single_service",
        })
        self.assertEqual(
            self.db.execute("SELECT * FROM incidents_validated ORDER BY incident_id").fetchall(),
            snapshot,
        )
        last = warehouse.status(self.db)["last_quality_run"]
        self.assertEqual(last["status"], "failed")

    def test_each_fault_triggers_its_own_check(self):
        faulty, faults = events.inject_faults(events.generate_batch(DAYS[-1]))
        self.load(DAYS[:-1])
        warehouse.load_batch(self.db, DAYS[-1], faulty)
        results = {item["check"]: item for item in warehouse.run_checks(self.db, AS_OF)}
        for check in faults:
            with self.subTest(check=check):
                self.assertFalse(results[check]["passed"])
                self.assertEqual(results[check]["failing_rows"], 1)
        self.assertTrue(results["assert_valid_status"]["passed"])
        self.assertTrue(results["assert_freshness"]["passed"])

    def test_invalid_status_is_detected(self):
        rows = events.generate_batch(DAYS[-1])
        rows[0] = {**rows[0], "status": "closed"}
        self.load(DAYS[:-1])
        warehouse.load_batch(self.db, DAYS[-1], rows)
        result = warehouse.transform(self.db, AS_OF)
        self.assertEqual(result["failed_checks"], ["assert_valid_status"])

    def test_stale_data_fails_freshness(self):
        self.load(DAYS)
        stale_as_of = events.format_ts(warehouse.to_utc(AS_OF) + timedelta(days=5))
        result = warehouse.transform(self.db, stale_as_of)
        self.assertEqual(result["failed_checks"], ["assert_freshness"])

    def test_reprocess_range_repairs_bad_partitions(self):
        self.load(DAYS)
        good = warehouse.transform(self.db, AS_OF)
        for day in DAYS[-3:]:
            faulty, _ = events.inject_faults(events.generate_batch(day))
            warehouse.load_batch(self.db, day, faulty)
        self.assertEqual(warehouse.transform(self.db, AS_OF)["status"], "failed")

        landing = self.tmp / "landing"
        argv = [
            "reprocess", "--start", DAYS[-3].isoformat(), "--end", DAYS[-1].isoformat(),
            "--db", str(self.tmp / "test.db"), "--landing", str(landing), "--as-of", AS_OF,
        ]
        with redirect_stdout(io.StringIO()) as output:
            exit_code = pipeline.main(argv)
        report = json.loads(output.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual([load["batch_date"] for load in report["loads"]],
                         [day.isoformat() for day in DAYS[-3:]])
        self.assertEqual(report["transform"]["validated_rows"], good["validated_rows"])

    def test_demo_scenario_passes(self):
        with redirect_stdout(io.StringIO()) as output:
            exit_code = pipeline.main(["demo", "--demo-dir", str(self.tmp / "demo")])
        report = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0, report["expectations"])
        self.assertTrue(all(report["expectations"].values()), report["expectations"])


class DataformParityTest(unittest.TestCase):
    """Local checks and Dataform assertions must stay in sync."""

    def test_every_local_check_has_a_dataform_assertion(self):
        files = {path.stem for path in (ROOT / "dataform" / "definitions" / "assertions").glob("*.sqlx")}
        self.assertEqual(files, set(warehouse.CHECKS))

    def test_validated_table_depends_on_every_assertion(self):
        source = (ROOT / "dataform" / "definitions" / "marts" / "incidents_validated.sqlx").read_text()
        config = source.split("}", 1)[0]
        declared = set(re.findall(r'"(assert_[a-z_]+)"', config))
        self.assertEqual(declared, set(warehouse.CHECKS))

    def test_accepted_values_match_generator(self):
        settings = (ROOT / "dataform" / "workflow_settings.yaml").read_text()
        for value in events.SEVERITIES + events.STATUSES:
            self.assertIn(value, settings)


if __name__ == "__main__":
    unittest.main()
